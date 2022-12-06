package com.vmturbo.topology.processor.cost;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Streams;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostBucket;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostItem;
import com.vmturbo.common.protobuf.cost.BilledCostServiceGrpc;
import com.vmturbo.common.protobuf.cost.BilledCostServiceGrpc.BilledCostServiceImplBase;
import com.vmturbo.common.protobuf.cost.BilledCostServiceGrpc.BilledCostServiceStub;
import com.vmturbo.common.protobuf.cost.BilledCostServices.UploadBilledCloudCostRequest;
import com.vmturbo.common.protobuf.cost.BilledCostServices.UploadBilledCloudCostRequest.BilledCostSegment;
import com.vmturbo.common.protobuf.cost.BilledCostServices.UploadBilledCloudCostRequest.CostTagsSegment;
import com.vmturbo.common.protobuf.cost.BilledCostServices.UploadBilledCloudCostRequest.MetadataSegment;
import com.vmturbo.common.protobuf.cost.BilledCostServices.UploadBilledCloudCostResponse;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CommonCost.PriceModel;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData.CloudBillingBucket;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData.CloudBillingBucket.Granularity;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint.CostCategory;
import com.vmturbo.platform.sdk.common.CostBilling.CostTagGroup;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.stitching.StitchingContext;

/**
 * Test class for {@link BilledCloudCostUploader}.
 */
public class BilledCloudCostUploaderTest {

    private static final CloudBillingDataPoint VIRTUAL_MACHINE_USAGE_1 =
            CloudBillingDataPoint.newBuilder()
                    .setEntityId("s89dafsdf-c20a-4868-r23-8s9f7aew9sf/CLOUD-PAAS-RG/paasstressvm2")
                    .setCloudServiceId("azure::CS::VirtualMachines")
                    .setAccountId("s89dafsdf-c20a-4868-r23-8s9f7aew9sf")
                    .setRegionId("azure::canadacentral::DC::canadacentral")
                    .setProviderId("azure::VMPROFILE::Standard_B2s")
                    .setEntityType(10)
                    .setPriceModel(PriceModel.ON_DEMAND)
                    .setCostCategory(CostCategory.COMPUTE)
                    .setUsageAmount(21)
                    .setCost(CurrencyAmount.newBuilder().setAmount(0.974047261247501).build())
                    .setCostTagGroupId(0)
                    .setSkuId("f024f557-b908-4e28-9f51-212ae32b97a3")
                    .setProviderType(56)
                    .setTrackChanges(true)
                    .setServiceProviderId("SERVICE_PROVIDER::Azure")
                    .build();

    private static final CloudBillingDataPoint VIRTUAL_MACHINE_USAGE_2 =
            VIRTUAL_MACHINE_USAGE_1.toBuilder()
                    .setServiceProviderId("SERVICE_PROVIDER::GCP")
                    .setCostTagGroupId(1)
                    .build();

    private static final CostTagGroup VIRTUAL_MACHINE_COST_TAG_GROUP_1 = CostTagGroup.newBuilder()
            .putTags(" turbo_lifetime", "99")
            .putTags("Login", "User")
            .putTags("turbo_owner", "turbonomic.com")
            .build();

    private static final CostTagGroup VIRTUAL_MACHINE_COST_TAG_GROUP_2 = CostTagGroup.newBuilder()
            .putTags("turbo_lifetime", "31")
            .putTags("turbo_owner", "turbonomic.com")
            .putTags("turbo_owner", "eng.turbonomic.com")
            .build();

    private final TestBilledCostRpcService billedCostServiceSpy =
            Mockito.spy(new TestBilledCostRpcService());

    /**
     * Stand up a GRPC server for testing.
     */
    @Rule
    public final GrpcTestServer server = GrpcTestServer.newServer(billedCostServiceSpy);

    private BilledCostServiceStub billedCostServiceStub;

    private StitchingContext stitchingContext;

    private CloudEntitiesMap cloudEntitiesMap;

    private PseudoOidService pseudoOidService;

    /**
     * Runs before each test.
     */
    @Before
    public void setup() {
        stitchingContext = Mockito.mock(StitchingContext.class);
        cloudEntitiesMap = Mockito.mock(CloudEntitiesMap.class);
        billedCostServiceStub = BilledCostServiceGrpc.newStub(server.getChannel());
        pseudoOidService = new PseudoOidService();
        billedCostServiceSpy.reset();
    }

    /**
     * Verify that a request is successfully sent containing multiple cost tag groups (but not
     * necessarily chunked).
     */
    @Test
    public void testRequestWithMultipleTagGroups() {
        final long targetId = 0L;
        final SDKProbeType probeType = SDKProbeType.AWS_CLOUD_BILLING;

        final CloudBillingData cloudBillingData = CloudBillingData.newBuilder()
                .setBillingIdentifier("47111111")
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664064000000L)
                        .addSamples(VIRTUAL_MACHINE_USAGE_1)
                        .addSamples(VIRTUAL_MACHINE_USAGE_1))
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664070000000L)
                        .addSamples(VIRTUAL_MACHINE_USAGE_2))
                .putCostTagGroupMap(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId(),
                        VIRTUAL_MACHINE_COST_TAG_GROUP_1) // tag group 1
                .putCostTagGroupMap(VIRTUAL_MACHINE_USAGE_2.getCostTagGroupId(),
                        VIRTUAL_MACHINE_COST_TAG_GROUP_2) // tag group 2
                .build();

        // Set up the BilledCostUploader
        final BilledCloudCostUploader billedCloudCostUploader =
                Mockito.spy(new BilledCloudCostUploader(billedCostServiceStub, 1024 * 1024, 30));

        // Set up CloudEntitiesMap (register fake OIDs)
        final Map<String, Long> entityBillingIdToOidMap = new HashMap<>();
        registerOids(cloudEntitiesMap, entityBillingIdToOidMap, cloudBillingData);

        doReturn(cloudEntitiesMap).when(billedCloudCostUploader)
                .createCloudEntitiesMap(any(), any());
        doReturn(entityBillingIdToOidMap).when(billedCloudCostUploader)
                .createEntityBillingIdToOidMap(any());

        // Execute the test actions
        billedCloudCostUploader.enqueueTargetBillingData(targetId, probeType,
                ImmutableList.of(cloudBillingData));

        billedCloudCostUploader.processUploadQueue(stitchingContext);

        // Extract the captured, chunked requests
        final List<UploadBilledCloudCostRequest> interceptedRequests =
                billedCostServiceSpy.getAllRequests();

        validateMetadataSegment(collectAllMetadataSegments(interceptedRequests).get(0),
                pseudoOidService.getOid(VIRTUAL_MACHINE_USAGE_1.getServiceProviderId()),
                pseudoOidService.getOid(cloudBillingData.getBillingIdentifier()),
                Granularity.HOURLY);

        final List<BilledCostBucket> billedCostBuckets =
                collectAllBilledCostBuckets(interceptedRequests);
        final List<BilledCostItem> billedCostItems = collectAllBilledCostItems(billedCostBuckets);
        final Map<Long, CostTagGroup> costTagGroups = collectAllCostTagGroups(interceptedRequests);

        assertEquals(cloudBillingData.getCloudCostBucketsCount(), billedCostBuckets.size());

        assertEquals(cloudBillingData.getCloudCostBucketsList()
                .stream()
                .map(CloudBillingBucket::getSamplesList)
                .mapToInt(List::size)
                .sum(), billedCostItems.size());

        assertEquals(cloudBillingData.getCostTagGroupMapMap(), costTagGroups);
    }

    /**
     * Try to upload some billing data with no cost tag groups. Expect that a cost tags upload request will be sent, but
     * expect that it is empty.
     */
    @Test
    public void testRequestWithNoTagGroups() {

        final long targetId = 0L;
        final SDKProbeType probeType = SDKProbeType.AWS_CLOUD_BILLING;

        final CloudBillingData cloudBillingData = CloudBillingData.newBuilder()
                .setBillingIdentifier("47111111")
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664064000000L)
                        .addSamples(VIRTUAL_MACHINE_USAGE_1.toBuilder().clearCostTagGroupId().build()))
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664070000000L)
                        .addSamples(VIRTUAL_MACHINE_USAGE_2.toBuilder().clearCostTagGroupId().build()))
                .build();

        // Set up the BilledCostUploader
        final BilledCloudCostUploader billedCloudCostUploader =
                Mockito.spy(new BilledCloudCostUploader(billedCostServiceStub, 1024 * 1024, 30));

        // Set up CloudEntitiesMap (register fake OIDs)
        final Map<String, Long> entityBillingIdToOidMap = new HashMap<>();
        registerOids(cloudEntitiesMap, entityBillingIdToOidMap, cloudBillingData);

        doReturn(cloudEntitiesMap).when(billedCloudCostUploader)
                .createCloudEntitiesMap(any(), any());
        doReturn(entityBillingIdToOidMap).when(billedCloudCostUploader)
                .createEntityBillingIdToOidMap(any());

        // Execute the test actions
        billedCloudCostUploader.enqueueTargetBillingData(targetId, probeType,
                ImmutableList.of(cloudBillingData));

        billedCloudCostUploader.processUploadQueue(stitchingContext);

        // Extract the captured, chunked requests
        final List<UploadBilledCloudCostRequest> interceptedRequests =
                billedCostServiceSpy.getAllRequests();

        // Despite not including any cost tag groups, there should have been an
        // empty cost tag groups segment sent
        final List<CostTagsSegment> costTagsSegments = interceptedRequests.stream()
                .filter(UploadBilledCloudCostRequest::hasCostTags)
                .map(UploadBilledCloudCostRequest::getCostTags)
                .collect(Collectors.toList());

        assertEquals(1, costTagsSegments.size());
        assertEquals(0, costTagsSegments.get(0).getCostTagGroupCount());
    }

    /**
     * Verify that a request is successfully sent containing multiple cost buckets (but not
     * necessarily chunked).
     */
    @Test
    public void testRequestWithMultipleBuckets() {
        final long targetId = 0L;
        final SDKProbeType probeType = SDKProbeType.AWS_CLOUD_BILLING;

        final CloudBillingData cloudBillingData = CloudBillingData.newBuilder()
                .setBillingIdentifier("47111111")
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664064000000L)
                        .addSamples(VIRTUAL_MACHINE_USAGE_1))
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664070000000L)
                        .addSamples(VIRTUAL_MACHINE_USAGE_1))
                .putCostTagGroupMap(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId(),
                        VIRTUAL_MACHINE_COST_TAG_GROUP_1)
                .build();

        // Set up the BilledCostUploader
        final BilledCloudCostUploader billedCloudCostUploader =
                Mockito.spy(new BilledCloudCostUploader(billedCostServiceStub, 1024 * 1024, 30));

        // Set up CloudEntitiesMap (register fake OIDs)
        final Map<String, Long> entityBillingIdToOidMap = new HashMap<>();
        registerOids(cloudEntitiesMap, entityBillingIdToOidMap, cloudBillingData);

        doReturn(cloudEntitiesMap).when(billedCloudCostUploader)
                .createCloudEntitiesMap(any(), any());
        doReturn(entityBillingIdToOidMap).when(billedCloudCostUploader)
                .createEntityBillingIdToOidMap(any());

        // Execute the test actions
        billedCloudCostUploader.enqueueTargetBillingData(targetId, probeType,
                ImmutableList.of(cloudBillingData));

        billedCloudCostUploader.processUploadQueue(stitchingContext);

        // Extract the captured, chunked requests
        final List<UploadBilledCloudCostRequest> interceptedRequests =
                billedCostServiceSpy.getAllRequests();

        validateMetadataSegment(collectAllMetadataSegments(interceptedRequests).get(0),
                pseudoOidService.getOid(VIRTUAL_MACHINE_USAGE_1.getServiceProviderId()),
                pseudoOidService.getOid(cloudBillingData.getBillingIdentifier()),
                Granularity.HOURLY);

        final List<BilledCostBucket> billedCostBuckets =
                collectAllBilledCostBuckets(interceptedRequests);
        final List<BilledCostItem> billedCostItems = collectAllBilledCostItems(billedCostBuckets);
        final Map<Long, CostTagGroup> costTagGroups = collectAllCostTagGroups(interceptedRequests);

        assertEquals(cloudBillingData.getCloudCostBucketsCount(), billedCostBuckets.size());

        assertEquals(cloudBillingData.getCloudCostBucketsList()
                .stream()
                .map(CloudBillingBucket::getSamplesList)
                .mapToInt(List::size)
                .sum(), billedCostItems.size());

        billedCostItems.forEach(item -> validateBilledCostItem(VIRTUAL_MACHINE_USAGE_1, item));

        assertEquals(cloudBillingData.getCostTagGroupMapMap(), costTagGroups);
    }

    /**
     * Pretend 2 targets have finished discovery and enqueued their billing data. Verify that all of
     * it is sent upon initiating an upload.
     */
    @Test
    public void testSimultaneousUploadForMultipleTargets() {

        final CloudBillingData cloudBillingDataAzure = CloudBillingData.newBuilder()
                .setBillingIdentifier("47111111")
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664064000000L)
                        .addSamples(VIRTUAL_MACHINE_USAGE_1))
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664070000000L)
                        .addAllSamples(
                                Iterables.limit(Iterables.cycle(VIRTUAL_MACHINE_USAGE_1), 10)))
                .putCostTagGroupMap(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId(),
                        VIRTUAL_MACHINE_COST_TAG_GROUP_1)
                .build();

        final CloudBillingData cloudBillingDataGcp = CloudBillingData.newBuilder()
                .setBillingIdentifier("engineering")
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664070000000L)
                        .addSamples(VIRTUAL_MACHINE_USAGE_2))
                .putCostTagGroupMap(VIRTUAL_MACHINE_USAGE_2.getCostTagGroupId(),
                        VIRTUAL_MACHINE_COST_TAG_GROUP_2)
                .build();

        // Set up the BilledCostUploader
        final BilledCloudCostUploader billedCloudCostUploader =
                Mockito.spy(new BilledCloudCostUploader(billedCostServiceStub, 4096, 30));

        // Set up CloudEntitiesMap (register fake OIDs)
        final Map<String, Long> entityBillingIdToOidMap = new HashMap<>();
        registerOids(cloudEntitiesMap, entityBillingIdToOidMap, cloudBillingDataAzure);
        registerOids(cloudEntitiesMap, entityBillingIdToOidMap, cloudBillingDataGcp);

        doReturn(cloudEntitiesMap).when(billedCloudCostUploader)
                .createCloudEntitiesMap(any(), any());
        doReturn(entityBillingIdToOidMap).when(billedCloudCostUploader)
                .createEntityBillingIdToOidMap(any());

        // Execute the test actions
        billedCloudCostUploader.enqueueTargetBillingData(0L, SDKProbeType.AZURE_BILLING,
                ImmutableList.of(cloudBillingDataAzure));
        billedCloudCostUploader.enqueueTargetBillingData(1L, SDKProbeType.GCP_BILLING,
                ImmutableList.of(cloudBillingDataGcp));

        billedCloudCostUploader.processUploadQueue(stitchingContext);

        // Extract the captured, chunked requests
        final List<UploadBilledCloudCostRequest> interceptedRequests =
                billedCostServiceSpy.getAllRequests();

        final List<MetadataSegment> metadataSegments =
                collectAllMetadataSegments(interceptedRequests);

        assertEquals(2, metadataSegments.size());

        final Map<Long, List<MetadataSegment>> metadataByBillingId = metadataSegments.stream()
                .collect(Collectors.groupingBy(MetadataSegment::getBillingFamilyId));

        validateMetadataSegment(metadataByBillingId.get(
                        pseudoOidService.getOid(cloudBillingDataAzure.getBillingIdentifier())).get(0),
                pseudoOidService.getOid(VIRTUAL_MACHINE_USAGE_1.getServiceProviderId()),
                pseudoOidService.getOid(cloudBillingDataAzure.getBillingIdentifier()),
                Granularity.HOURLY);

        validateMetadataSegment(metadataByBillingId.get(
                        pseudoOidService.getOid(cloudBillingDataGcp.getBillingIdentifier())).get(0),
                pseudoOidService.getOid(VIRTUAL_MACHINE_USAGE_2.getServiceProviderId()),
                pseudoOidService.getOid(cloudBillingDataGcp.getBillingIdentifier()),
                Granularity.HOURLY);

        final List<BilledCostBucket> billedCostBuckets =
                collectAllBilledCostBuckets(interceptedRequests);
        final List<BilledCostItem> billedCostItems = collectAllBilledCostItems(billedCostBuckets);
        final Map<Long, CostTagGroup> costTagGroups = collectAllCostTagGroups(interceptedRequests);

        assertEquals(cloudBillingDataGcp.getCloudCostBucketsCount()
                + cloudBillingDataAzure.getCloudCostBucketsCount(), billedCostBuckets.size());

        assertEquals(Streams.concat(cloudBillingDataGcp.getCloudCostBucketsList().stream(),
                        cloudBillingDataAzure.getCloudCostBucketsList().stream())
                .map(CloudBillingBucket::getSamplesList)
                .mapToInt(List::size)
                .sum(), billedCostItems.size());

        final Map<Long, List<BilledCostItem>> itemsByCostTagGroupId = billedCostItems.stream()
                .collect(Collectors.groupingBy(BilledCostItem::getCostTagGroupId));

        validateBilledCostItem(VIRTUAL_MACHINE_USAGE_1,
                itemsByCostTagGroupId.get(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId()).get(0));

        validateBilledCostItem(VIRTUAL_MACHINE_USAGE_2,
                itemsByCostTagGroupId.get(VIRTUAL_MACHINE_USAGE_2.getCostTagGroupId()).get(0));

        assertEquals(ImmutableMap.builder()
                .putAll(cloudBillingDataGcp.getCostTagGroupMapMap())
                .putAll(cloudBillingDataAzure.getCostTagGroupMapMap())
                .build(), costTagGroups);
    }

    /**
     * Initiate an upload request with data that will not fit within the max chunk size, and verify
     * that the chunked requests contain all of the data.
     *
     * <p>It's hard to guarantee because protobuf object sizes are not deterministic, but we can say
     * with almost certainty that this request will be chunked (on any common machine).
     */
    @Test
    public void testRequestWithExpectedChunking() {
        final long targetId = 0L;
        final SDKProbeType probeType = SDKProbeType.AWS_CLOUD_BILLING;

        final CloudBillingData cloudBillingData = CloudBillingData.newBuilder()
                .setBillingIdentifier("47111111")
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664064000000L)
                        .addAllSamples(Iterables.limit(Iterables.cycle(VIRTUAL_MACHINE_USAGE_1),
                                20))) // ~1k bytes
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664070000000L)
                        .addAllSamples(
                                Iterables.limit(Iterables.cycle(VIRTUAL_MACHINE_USAGE_2), 20)))
                .putCostTagGroupMap(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId(),
                        VIRTUAL_MACHINE_COST_TAG_GROUP_1)
                .putCostTagGroupMap(VIRTUAL_MACHINE_USAGE_2.getCostTagGroupId(),
                        VIRTUAL_MACHINE_COST_TAG_GROUP_2)
                .build();

        // Set up the BilledCostUploader
        final BilledCloudCostUploader billedCloudCostUploader =
                Mockito.spy(new BilledCloudCostUploader(billedCostServiceStub, 1500, 30));

        // Set up CloudEntitiesMap (register fake OIDs)
        final Map<String, Long> entityBillingIdToOidMap = new HashMap<>();
        registerOids(cloudEntitiesMap, entityBillingIdToOidMap, cloudBillingData);

        doReturn(cloudEntitiesMap).when(billedCloudCostUploader)
                .createCloudEntitiesMap(any(), any());
        doReturn(entityBillingIdToOidMap).when(billedCloudCostUploader)
                .createEntityBillingIdToOidMap(any());

        // Execute the test actions
        billedCloudCostUploader.enqueueTargetBillingData(targetId, probeType,
                ImmutableList.of(cloudBillingData));

        billedCloudCostUploader.processUploadQueue(stitchingContext);

        // Extract the captured, chunked requests
        final List<UploadBilledCloudCostRequest> interceptedRequests =
                billedCostServiceSpy.getAllRequests();

        validateMetadataSegment(collectAllMetadataSegments(interceptedRequests).get(0),
                pseudoOidService.getOid(VIRTUAL_MACHINE_USAGE_1.getServiceProviderId()),
                pseudoOidService.getOid(cloudBillingData.getBillingIdentifier()),
                Granularity.HOURLY);

        final List<BilledCostBucket> billedCostBuckets =
                collectAllBilledCostBuckets(interceptedRequests);
        final List<BilledCostItem> billedCostItems = collectAllBilledCostItems(billedCostBuckets);
        final Map<Long, CostTagGroup> costTagGroups = collectAllCostTagGroups(interceptedRequests);

        assertEquals(cloudBillingData.getCloudCostBucketsCount(), billedCostBuckets.size());

        assertEquals(cloudBillingData.getCloudCostBucketsList()
                .stream()
                .map(CloudBillingBucket::getSamplesList)
                .mapToInt(List::size)
                .sum(), billedCostItems.size());

        assertEquals(cloudBillingData.getCostTagGroupMapMap(), costTagGroups);
    }

    /**
     * Send a request containing a billing bucket larger than the maximum request size. Expect that
     * that upload will fail, but uploads for other targets will succeed.
     */
    @Test
    public void testChunkTooLargeDoesNotFailUpload() {

        final CloudBillingData cloudBillingDataAzureLarge = CloudBillingData.newBuilder()
                .setBillingIdentifier("47111111")
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664064000000L)
                        .addSamples(VIRTUAL_MACHINE_USAGE_1))
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664070000000L)
                        .addAllSamples(
                                Iterables.limit(Iterables.cycle(VIRTUAL_MACHINE_USAGE_1), 10)))
                .putCostTagGroupMap(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId(),
                        VIRTUAL_MACHINE_COST_TAG_GROUP_1)
                .build();

        final CloudBillingData cloudBillingDataGcpSmall = CloudBillingData.newBuilder()
                .setBillingIdentifier("engineering")
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664064000000L)
                        .addSamples(VIRTUAL_MACHINE_USAGE_1))
                .putCostTagGroupMap(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId(),
                        VIRTUAL_MACHINE_COST_TAG_GROUP_1)
                .build();

        // Set up the BilledCostUploader
        final BilledCloudCostUploader billedCloudCostUploader =
                Mockito.spy(new BilledCloudCostUploader(billedCostServiceStub, 128, 30));

        // Set up CloudEntitiesMap (register fake OIDs)
        final Map<String, Long> entityBillingIdToOidMap = new HashMap<>();
        registerOids(cloudEntitiesMap, entityBillingIdToOidMap, cloudBillingDataAzureLarge);
        registerOids(cloudEntitiesMap, entityBillingIdToOidMap, cloudBillingDataGcpSmall);

        doReturn(cloudEntitiesMap).when(billedCloudCostUploader)
                .createCloudEntitiesMap(any(), any());
        doReturn(entityBillingIdToOidMap).when(billedCloudCostUploader)
                .createEntityBillingIdToOidMap(any());

        // Execute the test actions
        billedCloudCostUploader.enqueueTargetBillingData(0L, SDKProbeType.AZURE_BILLING,
                ImmutableList.of(cloudBillingDataAzureLarge));
        billedCloudCostUploader.enqueueTargetBillingData(1L, SDKProbeType.GCP_BILLING,
                ImmutableList.of(cloudBillingDataGcpSmall));

        billedCloudCostUploader.processUploadQueue(stitchingContext);

        // Extract the captured, chunked requests
        final List<UploadBilledCloudCostRequest> interceptedRequests =
                billedCostServiceSpy.getAllRequests();

        final List<MetadataSegment> metadataSegments =
                collectAllMetadataSegments(interceptedRequests);

        // Only 1 of 2 uploads should succeed - we don't want incomplete data to be uploaded
        assertEquals(1, metadataSegments.size());
        validateMetadataSegment(metadataSegments.get(0),
                pseudoOidService.getOid(VIRTUAL_MACHINE_USAGE_1.getServiceProviderId()),
                pseudoOidService.getOid(cloudBillingDataGcpSmall.getBillingIdentifier()),
                Granularity.HOURLY);

        final List<BilledCostBucket> billedCostBuckets =
                collectAllBilledCostBuckets(interceptedRequests);
        final List<BilledCostItem> billedCostItems = collectAllBilledCostItems(billedCostBuckets);
        final Map<Long, CostTagGroup> costTagGroups = collectAllCostTagGroups(interceptedRequests);

        assertEquals(cloudBillingDataGcpSmall.getCloudCostBucketsCount(), billedCostBuckets.size());

        assertEquals(cloudBillingDataGcpSmall.getCloudCostBucketsList()
                .stream()
                .map(CloudBillingBucket::getSamplesList)
                .mapToInt(List::size)
                .sum(), billedCostItems.size());

        assertEquals(cloudBillingDataGcpSmall.getCostTagGroupMapMap(), costTagGroups);
    }

    /**
     * Test that a bucket with too many data points to fit in a request gets uploaded somehow.
     */
    @Test
    public void testOversizedBucketGetsSplitAndUploaded() {
        // Select some test inputs
        final long targetId = 9999L;
        final SDKProbeType probeType = SDKProbeType.AZURE_BILLING;

        final CloudBillingData cloudBillingDataAzureLarge = CloudBillingData.newBuilder()
                .setBillingIdentifier("47111111")
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664064000000L)
                        .addSamples(VIRTUAL_MACHINE_USAGE_1))
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664070000000L)
                        .addAllSamples(
                                Iterables.limit(Iterables.cycle(VIRTUAL_MACHINE_USAGE_1), 20)))
                .putCostTagGroupMap(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId(),
                        VIRTUAL_MACHINE_COST_TAG_GROUP_1)
                .build();

        // Set up the BilledCostUploader
        final BilledCloudCostUploader billedCloudCostUploader =
                Mockito.spy(new BilledCloudCostUploader(billedCostServiceStub, 256, 30));

        // Set up CloudEntitiesMap (register fake OIDs)
        final Map<String, Long> entityBillingIdToOidMap = new HashMap<>();
        registerOids(cloudEntitiesMap, entityBillingIdToOidMap, cloudBillingDataAzureLarge);

        doReturn(cloudEntitiesMap).when(billedCloudCostUploader)
                .createCloudEntitiesMap(any(), any());
        doReturn(entityBillingIdToOidMap).when(billedCloudCostUploader)
                .createEntityBillingIdToOidMap(any());

        // Execute the test actions
        billedCloudCostUploader.enqueueTargetBillingData(targetId, probeType,
                ImmutableList.of(cloudBillingDataAzureLarge));

        billedCloudCostUploader.processUploadQueue(stitchingContext);

        // Extract the captured, chunked requests
        final List<UploadBilledCloudCostRequest> interceptedRequests =
                billedCostServiceSpy.getAllRequests();

        // Verify that metadata was sent
        final MetadataSegment metadata = collectAllMetadataSegments(interceptedRequests).get(0);

        assertEquals(pseudoOidService.getOid(cloudBillingDataAzureLarge.getBillingIdentifier()),
                metadata.getBillingFamilyId());
        assertEquals(pseudoOidService.getOid(VIRTUAL_MACHINE_USAGE_1.getServiceProviderId()),
                metadata.getServiceProviderId());
        assertEquals(Granularity.HOURLY, metadata.getGranularity()); // HOURLY is default

        // Verify that cost tags were sent (there should just be 1)
        final Map<Long, CostTagGroup> costTagGroupsById =
                collectAllCostTagGroups(interceptedRequests);

        assertEquals(1, costTagGroupsById.size());
        assertEquals(VIRTUAL_MACHINE_COST_TAG_GROUP_1,
                costTagGroupsById.get(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId()));

        // Verify that some buckets got split up
        final List<BilledCostBucket> billedCostBuckets =
                collectAllBilledCostBuckets(interceptedRequests);

        assertTrue(
                billedCostBuckets.size() > cloudBillingDataAzureLarge.getCloudCostBucketsCount());

        // Verify that the buckets all have timestamps
        for (final BilledCostBucket billedCostBucket : billedCostBuckets) {
            assertTrue(billedCostBucket.hasSampleTsUtc());
        }

        // Verify that all the items made it, one way or another
        final List<BilledCostItem> billedCostItems = billedCostBuckets.stream()
                .map(BilledCostBucket::getCostItemsList)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        assertEquals(21, billedCostItems.size());

        for (final BilledCostItem billedCostItem : billedCostItems) {
            assertEquals(pseudoOidService.getOid(VIRTUAL_MACHINE_USAGE_1.getCloudServiceId()),
                    billedCostItem.getCloudServiceId());
            assertEquals(pseudoOidService.getOid(VIRTUAL_MACHINE_USAGE_1.getAccountId()),
                    billedCostItem.getAccountId());
            assertEquals(pseudoOidService.getOid(VIRTUAL_MACHINE_USAGE_1.getRegionId()),
                    billedCostItem.getRegionId());
        }
    }

    /**
     * Test the happy path with no expected chunking and just 1 cost bucket and just 1 cost tag
     * group.
     */
    @Test
    public void testBasicRequest() {
        // Select some test inputs
        final long targetId = 9999L;
        final SDKProbeType probeType = SDKProbeType.AZURE_BILLING;

        final CloudBillingData cloudBillingData = CloudBillingData.newBuilder()
                .setBillingIdentifier("47111111")
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664064000000L)
                        .addSamples(VIRTUAL_MACHINE_USAGE_1))
                .putCostTagGroupMap(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId(),
                        VIRTUAL_MACHINE_COST_TAG_GROUP_1)
                .build();

        // Set up the BilledCostUploader
        final BilledCloudCostUploader billedCloudCostUploader =
                Mockito.spy(new BilledCloudCostUploader(billedCostServiceStub, 1024 * 1024, 30));

        // Set up CloudEntitiesMap (register fake OIDs)
        final Map<String, Long> entityBillingIdToOidMap = new HashMap<>();
        registerOids(cloudEntitiesMap, entityBillingIdToOidMap, cloudBillingData);

        doReturn(cloudEntitiesMap).when(billedCloudCostUploader)
                .createCloudEntitiesMap(any(), any());
        doReturn(entityBillingIdToOidMap).when(billedCloudCostUploader)
                .createEntityBillingIdToOidMap(any());

        // Execute the test actions
        billedCloudCostUploader.enqueueTargetBillingData(targetId, probeType,
                ImmutableList.of(cloudBillingData));

        billedCloudCostUploader.processUploadQueue(stitchingContext);

        // Extract the captured, chunked requests
        final List<UploadBilledCloudCostRequest> interceptedRequests =
                billedCostServiceSpy.getAllRequests();

        // Verify that metadata was sent
        final MetadataSegment metadata = collectAllMetadataSegments(interceptedRequests).get(0);

        assertEquals(pseudoOidService.getOid(cloudBillingData.getBillingIdentifier()),
                metadata.getBillingFamilyId());
        assertEquals(pseudoOidService.getOid(VIRTUAL_MACHINE_USAGE_1.getServiceProviderId()),
                metadata.getServiceProviderId());
        assertEquals(Granularity.HOURLY, metadata.getGranularity()); // HOURLY is default

        // Verify that cost tags were sent (there should just be 1)
        final Map<Long, CostTagGroup> costTagGroupsById =
                collectAllCostTagGroups(interceptedRequests);

        assertEquals(1, costTagGroupsById.size());
        assertEquals(VIRTUAL_MACHINE_COST_TAG_GROUP_1,
                costTagGroupsById.get(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId()));

        // Verify that billing buckets were sent (there should just be 1 with 1 item inside)

        final List<BilledCostBucket> billedCostBuckets =
                collectAllBilledCostBuckets(interceptedRequests);

        assertEquals(1, billedCostBuckets.size());

        final BilledCostBucket billedCostBucket = billedCostBuckets.get(0);

        assertEquals(1, billedCostBucket.getCostItemsCount());
        final BilledCostItem billedCostItem = billedCostBucket.getCostItems(0);

        assertEquals(pseudoOidService.getOid(VIRTUAL_MACHINE_USAGE_1.getCloudServiceId()),
                billedCostItem.getCloudServiceId());
        assertEquals(pseudoOidService.getOid(VIRTUAL_MACHINE_USAGE_1.getAccountId()),
                billedCostItem.getAccountId());
        assertEquals(pseudoOidService.getOid(VIRTUAL_MACHINE_USAGE_1.getRegionId()),
                billedCostItem.getRegionId());
    }

    /**
     * If the receiver of the upload request indicates an error, make sure that the current
     * target's billing data fails to upload. Additionally, expect that all of the target's failed
     * uploads will be tried again in the next upload cycle (assuming a re-discovery hasn't happened
     * yet, which will take precedence).
     */
    @Test
    public void testRemoteErrorDuringAllUploadsRetriesAllUploads() {

        // Select some test inputs
        final long targetId = 9999L;
        final SDKProbeType probeType = SDKProbeType.AZURE_BILLING;

        final CloudBillingData cloudBillingData1 = CloudBillingData.newBuilder()
                .setBillingIdentifier("47111111")
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664064000000L)
                        .addSamples(VIRTUAL_MACHINE_USAGE_1))
                .putCostTagGroupMap(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId(),
                        VIRTUAL_MACHINE_COST_TAG_GROUP_1)
                .build();

        final CloudBillingData cloudBillingData2 =
                cloudBillingData1.toBuilder().setBillingIdentifier("300003").build();

        // Set up the BilledCostUploader
        final BilledCloudCostUploader billedCloudCostUploader =
                Mockito.spy(new BilledCloudCostUploader(billedCostServiceStub, 1024 * 1024, 30));

        // Set up CloudEntitiesMap (register fake OIDs)
        final Map<String, Long> entityBillingIdToOidMap = new HashMap<>();
        registerOids(cloudEntitiesMap, entityBillingIdToOidMap, cloudBillingData1);

        doReturn(cloudEntitiesMap).when(billedCloudCostUploader)
                .createCloudEntitiesMap(any(), any());
        doReturn(entityBillingIdToOidMap).when(billedCloudCostUploader)
                .createEntityBillingIdToOidMap(any());

        // Add 2 upload operations for this target
        billedCloudCostUploader.enqueueTargetBillingData(targetId, probeType,
                ImmutableList.of(cloudBillingData1, cloudBillingData2));

        // Immediately emit an error when any request is received by the RPC service
        when(billedCostServiceSpy.uploadBilledCloudCost(any())).thenAnswer(
                uploadInvocation -> new TestResponseHandler(billedCostServiceSpy,
                        uploadInvocation.getArgumentAt(0, StreamObserver.class), true));

        billedCloudCostUploader.processUploadQueue(stitchingContext);

        // Make sure no successful requests were received
        assertEquals(0, billedCostServiceSpy.getAllRequests().size());

        // Check that the failed uploads were re-inserted into the queue
        final SetMultimap<Long, CloudBillingData> uploadQueue =
                ((SetMultimap<Long, CloudBillingData>)ReflectionTestUtils.getField(
                        billedCloudCostUploader, "targetBillingDataUploadQueue"));
        assertEquals(ImmutableSetMultimap.builder()
                .put(targetId, cloudBillingData1)
                .put(targetId, cloudBillingData2)
                .build(), uploadQueue);
    }

    /**
     * Fail 1 of 2 uploads for a target, and make sure 1 is received successfully and the other is
     * queued to be re-tried.
     */
    @Test
    public void testRemoteErrorDuringOneUploadRetriesOnlyOneUpload() {
        // Select some test inputs
        final long targetId = 9999L;
        final SDKProbeType probeType = SDKProbeType.AZURE_BILLING;

        final CloudBillingData cloudBillingData1 = CloudBillingData.newBuilder()
                .setBillingIdentifier("47111111")
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664064000000L)
                        .addSamples(VIRTUAL_MACHINE_USAGE_1))
                .putCostTagGroupMap(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId(),
                        VIRTUAL_MACHINE_COST_TAG_GROUP_1)
                .build();

        final CloudBillingData cloudBillingData2 =
                cloudBillingData1.toBuilder().setBillingIdentifier("300003").build();

        // Set up the BilledCostUploader
        final BilledCloudCostUploader billedCloudCostUploader =
                Mockito.spy(new BilledCloudCostUploader(billedCostServiceStub, 1024 * 1024, 30));

        // Set up CloudEntitiesMap (register fake OIDs)
        final Map<String, Long> entityBillingIdToOidMap = new HashMap<>();
        registerOids(cloudEntitiesMap, entityBillingIdToOidMap, cloudBillingData1);

        doReturn(cloudEntitiesMap).when(billedCloudCostUploader)
                .createCloudEntitiesMap(any(), any());
        doReturn(entityBillingIdToOidMap).when(billedCloudCostUploader)
                .createEntityBillingIdToOidMap(any());

        // Add 2 upload operations for this target
        billedCloudCostUploader.enqueueTargetBillingData(targetId, probeType,
                ImmutableList.of(cloudBillingData1, cloudBillingData2));

        // On the first upload, call the real method, but on the next upload, emit an error
        when(billedCostServiceSpy.uploadBilledCloudCost(any())).thenCallRealMethod()
                .thenAnswer(uploadInvocation -> new TestResponseHandler(billedCostServiceSpy,
                        uploadInvocation.getArgumentAt(0, StreamObserver.class), true));

        billedCloudCostUploader.processUploadQueue(stitchingContext);

        // At least one successful set of 3 request segments should have been received
        assertThat(billedCostServiceSpy.getAllRequests().size(), greaterThanOrEqualTo(3));

        // Check that the failed upload was re-inserted into the queue, but not the successful one
        final SetMultimap<Long, CloudBillingData> uploadQueue =
                ((SetMultimap<Long, CloudBillingData>)ReflectionTestUtils.getField(
                        billedCloudCostUploader, "targetBillingDataUploadQueue"));

        assertFalse(uploadQueue.get(targetId)
                .containsAll(ImmutableList.of(cloudBillingData1, cloudBillingData2)));
        assertThat(uploadQueue.get(targetId),
                either(contains(cloudBillingData1)).or(contains(cloudBillingData2)));
    }

    /**
     * Test that state is correctly restored from exported diags.
     *
     * @throws Exception on error
     */
    @Test
    public void testCollectAndRestoreDiagnostics() throws Exception {
        // Set up the BilledCostUploader
        final BilledCloudCostUploader billedCloudCostUploader =
                Mockito.spy(new BilledCloudCostUploader(billedCostServiceStub, 1024 * 1024, 30));

        // Enqueue some billing data
        final CloudBillingData cloudBillingData1 = CloudBillingData.newBuilder()
                .setBillingIdentifier("47111111")
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664064000000L)
                        .addSamples(VIRTUAL_MACHINE_USAGE_1))
                .putCostTagGroupMap(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId(),
                        VIRTUAL_MACHINE_COST_TAG_GROUP_1)
                .build();

        final CloudBillingData cloudBillingData2 =
                cloudBillingData1.toBuilder().setBillingIdentifier("300003").build();

        billedCloudCostUploader.enqueueTargetBillingData(1001L, SDKProbeType.GCP_BILLING,
                ImmutableList.of(cloudBillingData1, cloudBillingData2));

        billedCloudCostUploader.enqueueTargetBillingData(9999L, SDKProbeType.AZURE_BILLING,
                ImmutableList.of(cloudBillingData2));

        // Collect and restore diags
        final List<String> collectedDiags = new ArrayList<>();

        billedCloudCostUploader.collectDiags(collectedDiags::add);

        assertEquals(2, collectedDiags.size());

        billedCloudCostUploader.restoreDiags(collectedDiags, null);

        // Verify the state looks correct
        final SetMultimap<Long, CloudBillingData> uploadQueue =
                ((SetMultimap<Long, CloudBillingData>)ReflectionTestUtils.getField(
                        billedCloudCostUploader, "targetBillingDataUploadQueue"));

        final Map<Long, SDKProbeType> targetTypes =
                ((Map<Long, SDKProbeType>)ReflectionTestUtils.getField(billedCloudCostUploader,
                        "activeTargetProbeTypes"));

        assertEquals(
                ImmutableMap.of(1001L, SDKProbeType.GCP_BILLING, 9999L, SDKProbeType.AZURE_BILLING),
                targetTypes);

        assertEquals(ImmutableSetMultimap.builder()
                .putAll(1001L, cloudBillingData1, cloudBillingData2)
                .put(9999L, cloudBillingData2)
                .build(), uploadQueue);
    }

    /**
     * 8 bytes is far too small for a chunk of cost data, so require that it throws an exception.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSmallMaximumRequestSizeSetting() {
        new BilledCloudCostUploader(billedCostServiceStub, 8, 30);
    }

    /**
     * Providing a negative timeout doesn't make sense, so require that it throws an exception.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNegativeUploadRequestTimeoutSetting() {
        new BilledCloudCostUploader(billedCostServiceStub, 1024 * 1024, -1);
    }

    /**
     * Send entity-level cost for an entity with an unresolvable OID. Expect that it doesn't make it
     * into the database.
     */
    @Test
    public void testEntityCostForUndiscoveredEntityIsDropped() {
        // Select some test inputs
        final long targetId = 9999L;
        final SDKProbeType probeType = SDKProbeType.AZURE_BILLING;

        final CloudBillingData cloudBillingData = CloudBillingData.newBuilder()
                .setBillingIdentifier("47111111")
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .setTimestampUtcMillis(1664064000000L)
                        .addSamples(VIRTUAL_MACHINE_USAGE_1))
                .putCostTagGroupMap(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId(),
                        VIRTUAL_MACHINE_COST_TAG_GROUP_1)
                .build();

        // Set up the BilledCostUploader
        final BilledCloudCostUploader billedCloudCostUploader =
                Mockito.spy(new BilledCloudCostUploader(billedCostServiceStub, 1024 * 1024, 30));

        // Set up CloudEntitiesMap (register fake OIDs)
        final Map<String, Long> entityBillingIdToOidMap = new HashMap<>();
        registerOids(cloudEntitiesMap, entityBillingIdToOidMap, cloudBillingData);

        // Pretend there are no entity billing OIDs discovered
        entityBillingIdToOidMap.clear();

        doReturn(cloudEntitiesMap).when(billedCloudCostUploader)
                .createCloudEntitiesMap(any(), any());
        doReturn(entityBillingIdToOidMap).when(billedCloudCostUploader)
                .createEntityBillingIdToOidMap(any());

        // Execute the test actions
        billedCloudCostUploader.enqueueTargetBillingData(targetId, probeType,
                ImmutableList.of(cloudBillingData));

        billedCloudCostUploader.processUploadQueue(stitchingContext);

        // Extract the captured, chunked requests
        final List<UploadBilledCloudCostRequest> interceptedRequests =
                billedCostServiceSpy.getAllRequests();

        // Verify that metadata was sent
        final MetadataSegment metadata = collectAllMetadataSegments(interceptedRequests).get(0);

        assertEquals(pseudoOidService.getOid(cloudBillingData.getBillingIdentifier()),
                metadata.getBillingFamilyId());
        assertEquals(pseudoOidService.getOid(VIRTUAL_MACHINE_USAGE_1.getServiceProviderId()),
                metadata.getServiceProviderId());
        assertEquals(Granularity.HOURLY, metadata.getGranularity()); // HOURLY is default

        // Verify that cost tags were sent (there should just be 1)
        final Map<Long, CostTagGroup> costTagGroupsById =
                collectAllCostTagGroups(interceptedRequests);

        assertEquals(1, costTagGroupsById.size());
        assertEquals(VIRTUAL_MACHINE_COST_TAG_GROUP_1,
                costTagGroupsById.get(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId()));

        // Verify that a billing bucket was sent

        final List<BilledCostBucket> billedCostBuckets =
                collectAllBilledCostBuckets(interceptedRequests);

        assertEquals(1, billedCostBuckets.size());

        final BilledCostBucket billedCostBucket = billedCostBuckets.get(0);

        // But the only data point in the bucket should have been dropped

        assertEquals(0, billedCostBucket.getCostItemsCount());
    }

    private void registerOids(final CloudEntitiesMap cloudEntitiesMap,
            final Map<String, Long> entityBillingIdToOidMap,
            final CloudBillingData cloudBillingData) {
        when(cloudEntitiesMap.get(eq(cloudBillingData.getBillingIdentifier()))).thenReturn(
                pseudoOidService.getOid(cloudBillingData.getBillingIdentifier()));
        cloudBillingData.getCloudCostBucketsList().forEach(bucket -> {
            bucket.getSamplesList().forEach(item -> {
                when(cloudEntitiesMap.get(eq(item.getCloudServiceId()))).thenReturn(
                        pseudoOidService.getOid(item.getCloudServiceId()));
                when(cloudEntitiesMap.get(eq(item.getServiceProviderId()))).thenReturn(
                        pseudoOidService.getOid(item.getServiceProviderId()));
                when(cloudEntitiesMap.get(eq(item.getAccountId()))).thenReturn(
                        pseudoOidService.getOid(item.getAccountId()));
                when(cloudEntitiesMap.get(eq(item.getProviderId()))).thenReturn(
                        pseudoOidService.getOid(item.getProviderId()));
                when(cloudEntitiesMap.get(eq(item.getRegionId()))).thenReturn(
                        pseudoOidService.getOid(item.getRegionId()));
                entityBillingIdToOidMap.put(item.getEntityId(),
                        pseudoOidService.getOid(item.getEntityId()));
            });
        });
    }

    private void validateBilledCostItem(final CloudBillingDataPoint expectedDataPoint,
            final BilledCostItem actualBilledCostItem) {
        assertEquals(pseudoOidService.getOid(expectedDataPoint.getAccountId()),
                actualBilledCostItem.getAccountId());
        assertEquals(pseudoOidService.getOid(expectedDataPoint.getRegionId()),
                actualBilledCostItem.getRegionId());
        assertEquals(pseudoOidService.getOid(expectedDataPoint.getCloudServiceId()),
                actualBilledCostItem.getCloudServiceId());
        assertEquals(pseudoOidService.getOid(expectedDataPoint.getEntityId()),
                actualBilledCostItem.getEntityId());
        assertEquals(pseudoOidService.getOid(expectedDataPoint.getProviderId()),
                actualBilledCostItem.getProviderId());
        assertEquals(expectedDataPoint.getEntityType(), actualBilledCostItem.getEntityType());
        assertEquals(expectedDataPoint.getProviderType(), actualBilledCostItem.getProviderType());
        assertEquals(expectedDataPoint.getPriceModel(), actualBilledCostItem.getPriceModel());
        if (expectedDataPoint.hasPurchasedCommodity()) {
            assertEquals(expectedDataPoint.getPurchasedCommodity().getNumber(),
                    actualBilledCostItem.getCommodityType());
        }
        assertEquals(expectedDataPoint.getCost(), actualBilledCostItem.getCost());
        assertEquals(expectedDataPoint.getCostCategory(), actualBilledCostItem.getCostCategory());
        assertEquals(expectedDataPoint.getCostTagGroupId(),
                actualBilledCostItem.getCostTagGroupId());
        // sku id?
    }

    private List<MetadataSegment> collectAllMetadataSegments(
            final List<UploadBilledCloudCostRequest> requests) {
        return requests.stream()
                .filter(UploadBilledCloudCostRequest::hasBilledCostMetadata)
                .map(UploadBilledCloudCostRequest::getBilledCostMetadata)
                .collect(Collectors.toList());
    }

    private List<BilledCostBucket> collectAllBilledCostBuckets(
            final List<UploadBilledCloudCostRequest> requests) {
        return requests.stream()
                .filter(UploadBilledCloudCostRequest::hasBilledCost)
                .map(UploadBilledCloudCostRequest::getBilledCost)
                .map(BilledCostSegment::getCostBucketsList)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private List<BilledCostItem> collectAllBilledCostItems(
            final List<BilledCostBucket> billedCostBuckets) {
        return billedCostBuckets.stream()
                .map(BilledCostBucket::getCostItemsList)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private Map<Long, CostTagGroup> collectAllCostTagGroups(
            final List<UploadBilledCloudCostRequest> requests) {
        return requests.stream()
                .filter(UploadBilledCloudCostRequest::hasCostTags)
                .map(UploadBilledCloudCostRequest::getCostTags)
                .map(CostTagsSegment::getCostTagGroupMap)
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    private void validateMetadataSegment(final MetadataSegment metadataSegment,
            final long exepectedServiceProviderId, final long expectedBillingFamilyId,
            final Granularity expectedGranularity) {
        assertEquals(expectedBillingFamilyId, metadataSegment.getBillingFamilyId());
        assertEquals(exepectedServiceProviderId, metadataSegment.getServiceProviderId());
        assertEquals(expectedGranularity, metadataSegment.getGranularity());
    }

    /**
     * Generates fake OIDs and caches them so that the same value is returned for the same input on
     * subsequent invocations.
     */
    private static class PseudoOidService {

        Map<String, Long> oidCache = new HashMap<>();

        Supplier<Long> oidGenerator = new MutableLong()::getAndIncrement;

        /**
         * Get or create a unique identifier for the given input.
         *
         * @param identifier input
         * @return fake OID
         */
        public long getOid(final String identifier) {
            return oidCache.computeIfAbsent(identifier, id -> oidGenerator.get());
        }
    }

    /**
     * Mock implementation of the {@link BilledCostServiceStub} for testing.
     */
    private static class TestBilledCostRpcService extends BilledCostServiceImplBase {

        private final List<UploadBilledCloudCostRequest> requests = new ArrayList<>();

        private final List<Throwable> errors = new ArrayList<>();

        @Override
        public StreamObserver<UploadBilledCloudCostRequest> uploadBilledCloudCost(
                final StreamObserver<UploadBilledCloudCostResponse> responseObserver) {
            return new TestResponseHandler(this, responseObserver);
        }

        /**
         * Get all requests received by this service.
         *
         * @return all requests
         */
        public List<UploadBilledCloudCostRequest> getAllRequests() {
            return requests;
        }

        /**
         * Get all errors received by this service.
         *
         * @return all errors
         */
        public List<Throwable> getAllErrors() {
            return errors;
        }

        /**
         * Reset the service between tests.
         */
        public void reset() {
            requests.clear();
            errors.clear();
        }
    }

    /**
     * Test implementation of a {@link StreamObserver} for handling responses from the RPC service.
     */
    private static class TestResponseHandler
            implements StreamObserver<UploadBilledCloudCostRequest> {

        private final TestBilledCostRpcService testBilledCostRpcService;

        private final StreamObserver<UploadBilledCloudCostResponse> responseObserver;

        private final boolean alwaysThrow;

        /**
         * Constructor for {@link TestResponseHandler}.
         *
         * @param testBilledCostRpcService rpc service
         * @param responseObserver response acceptor
         */
        TestResponseHandler(final TestBilledCostRpcService testBilledCostRpcService,
                final StreamObserver<UploadBilledCloudCostResponse> responseObserver, final boolean alwaysThrow) {
            this.testBilledCostRpcService = testBilledCostRpcService;
            this.responseObserver = responseObserver;
            this.alwaysThrow = alwaysThrow;
        }

        /**
         * Constructor for a {@link TestResponseHandler} emits and error on every onNext() call.
         * @param testBilledCostRpcService rpc service
         * @param responseObserver response acceptor
         */
        TestResponseHandler(final TestBilledCostRpcService testBilledCostRpcService,
                final StreamObserver<UploadBilledCloudCostResponse> responseObserver) {
            this(testBilledCostRpcService, responseObserver, false);
        }

        @Override
        public void onNext(final UploadBilledCloudCostRequest request) {
            if (alwaysThrow) {
                responseObserver.onError(Status.INTERNAL.withDescription("Remote error").asException());
                return;
            }
            testBilledCostRpcService.requests.add(request);
        }

        @Override
        public void onError(final Throwable error) {
            testBilledCostRpcService.errors.add(error);
        }

        @Override
        public void onCompleted() {
            responseObserver.onNext(UploadBilledCloudCostResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }
}