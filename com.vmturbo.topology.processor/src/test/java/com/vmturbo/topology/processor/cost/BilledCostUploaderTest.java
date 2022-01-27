package com.vmturbo.topology.processor.cost;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.BilledCostUploadServiceGrpc;
import com.vmturbo.common.protobuf.cost.BilledCostUploadServiceGrpc.BilledCostUploadServiceImplBase;
import com.vmturbo.common.protobuf.cost.BilledCostUploadServiceGrpc.BilledCostUploadServiceStub;
import com.vmturbo.common.protobuf.cost.Cost.UploadBilledCostRequest;
import com.vmturbo.common.protobuf.cost.Cost.UploadBilledCostRequest.BillingDataPoint;
import com.vmturbo.common.protobuf.cost.Cost.UploadBilledCostRequest.CostTagGroupMap;
import com.vmturbo.common.protobuf.cost.Cost.UploadBilledCostResponse;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.mediation.hybrid.cloud.common.PropertyName;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData.CloudBillingBucket;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData.CloudBillingBucket.Granularity;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint;
import com.vmturbo.platform.sdk.common.CostBilling.CostTagGroup;
import com.vmturbo.topology.processor.cost.BilledCostUploader.BilledCostDataWrapper;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Test for BilledCostUploader.
 */
public class BilledCostUploaderTest {

    private TopologyInfo topologyInfo;

    private StitchingContext stitchingContext;

    private CloudEntitiesMap cloudEntitiesMap;

    private BilledCostUploader billedCostUploader;

    private static final String BILLING_IDENTIFIER = "1";
    private static final Long TOPOLOGY_ID = 12345L;
    private static final Long TIMESTAMP1 = 123456L;
    private static final Long TIMESTAMP2 = 456789L;
    private static final String VM_BILLING_ID = "vmBillingId";
    private static final String VM_LOCAL_ID = "vmId";
    private static final long VM_OID = 1L;
    private static final String DB_BILLING_ID = "dbBillingId";
    private static final String DB_LOCAL_ID = "dbId";
    private static final long DB_OID = 2L;
    private static final String VOLUME_LOCAL_ID = "volumeLocalId";
    private static final long VOLUME_OID = 3L;
    private static final String REGION_LOCAL_ID = "regionLocalId";
    private static final long REGION_OID = 123L;
    private static final String ACCOUNT_LOCAL_ID = "accountLocalId";
    private static final long ACCOUNT_OID = 456L;
    private static final String CLOUD_SERVICE_LOCAL_ID = "cloudServiceLocalId";
    private static final long CLOUD_SERVICE_OID = 789L;

    private final TestBilledCostUploadService billedCostUploadServiceSpy = spy(new TestBilledCostUploadService());
    /**
     * Test server.
     */
    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(billedCostUploadServiceSpy);

    private BilledCostUploadServiceStub billUploadServiceClient;

    private final StitchingEntityData vm = StitchingEntityData.newBuilder(EntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .setId(VM_LOCAL_ID)
            .addEntityProperties(EntityProperty.newBuilder()
                    .setNamespace("namespace")
                    .setName(PropertyName.BILLING_ID)
                    .setValue(VM_BILLING_ID)
                    .build())).oid(VM_OID).build();
    private final StitchingEntityData db = StitchingEntityData.newBuilder(EntityDTO.newBuilder()
            .setEntityType(EntityType.DATABASE)
            .setId(DB_LOCAL_ID)
            .addEntityProperties(EntityProperty.newBuilder()
                    .setNamespace("namespace")
                    .setName(PropertyName.BILLING_ID)
                    .setValue(DB_BILLING_ID)
                    .build())).oid(DB_OID).build();
    private final StitchingEntityData volume = StitchingEntityData.newBuilder(EntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_VOLUME)
            .setId(VOLUME_LOCAL_ID)).oid(VOLUME_OID).build();
    private final StitchingEntityData account = StitchingEntityData.newBuilder(EntityDTO.newBuilder()
            .setEntityType(EntityType.BUSINESS_ACCOUNT)
            .setId(ACCOUNT_LOCAL_ID)).oid(ACCOUNT_OID).build();
    private final StitchingEntityData region = StitchingEntityData.newBuilder(EntityDTO.newBuilder()
            .setEntityType(EntityType.REGION)
            .setId(REGION_LOCAL_ID)).oid(REGION_OID).build();
    private final StitchingEntityData cloudService = StitchingEntityData.newBuilder(EntityDTO.newBuilder()
            .setEntityType(EntityType.CLOUD_SERVICE)
            .setId(CLOUD_SERVICE_LOCAL_ID)).oid(CLOUD_SERVICE_OID).build();

    /**
     * set up.
     */
    @Before
    public void setup() {
        topologyInfo = TopologyInfo.newBuilder().setTopologyId(TOPOLOGY_ID).build();
        stitchingContext = setupStitchingContext();
        cloudEntitiesMap = new CloudEntitiesMap(stitchingContext, new HashMap<>());
        billUploadServiceClient = BilledCostUploadServiceGrpc.newStub(server.getChannel());
        billedCostUploader = new BilledCostUploader(billUploadServiceClient, 0.08f);
    }

    /**
     * Test converting CloudBillingData to BilledCostDataWrapper.
     */
    @Test
    public void testBuildBilledCostDataWrapper() {
        setupCloudBillingData();
        final List<BilledCostDataWrapper> billedCostDataWrappers
                = billedCostUploader.buildBilledCostDataWrappers(stitchingContext, cloudEntitiesMap, new HashMap<>());
        assertEquals(1, billedCostDataWrappers.size());
        final BilledCostDataWrapper billedCostDataWrapper = billedCostDataWrappers.get(0);
        assertEquals(BILLING_IDENTIFIER, billedCostDataWrapper.billingIdentifier);
        assertEquals(Granularity.DAILY, billedCostDataWrapper.granularity);
        // billing data points are grouped by two costTagGroupMaps
        assertEquals(2, billedCostDataWrapper.samplesByCostTagGroupMap.size());
        final Map.Entry<CostTagGroupMap, List<BillingDataPoint>> pair1
                = billedCostDataWrapper.samplesByCostTagGroupMap.entrySet().stream()
                .filter(entry -> entry.getKey().getGroupId() == 1L).findFirst().orElse(null);
        assertNotNull(pair1);
        // costTagGroupMap1 with VM and DB data points.
        final BillingDataPoint vmDataPoint = pair1.getValue().stream()
                        .filter(dp -> TIMESTAMP1 == dp.getTimestampUtcMillis())
                        .findFirst().orElse(null);
        assertNotNull(vmDataPoint);
        assertEquals(ACCOUNT_OID, vmDataPoint.getAccountOid());
        assertEquals(CLOUD_SERVICE_OID, vmDataPoint.getCloudServiceOid());
        assertEquals(REGION_OID, vmDataPoint.getRegionOid());
        assertEquals(VM_OID, vmDataPoint.getEntityOid());
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, vmDataPoint.getEntityType());
        final BillingDataPoint dbDataPoint = pair1.getValue().stream()
                .filter(dp -> TIMESTAMP2 == dp.getTimestampUtcMillis())
                .findFirst().orElse(null);
        assertNotNull(dbDataPoint);
        assertEquals(DB_OID, dbDataPoint.getEntityOid());
        final Map.Entry<CostTagGroupMap, List<BillingDataPoint>> pair2
                = billedCostDataWrapper.samplesByCostTagGroupMap.entrySet().stream()
                .filter(entry -> entry.getKey().getGroupId() == 2L).findFirst().orElse(null);
        assertNotNull(pair2);
        // costTagGroupMap2 with volume data point.
        assertEquals(1, pair2.getValue().size());
        final BillingDataPoint volumeDataPoint = pair2.getValue().get(0);
        assertEquals(VOLUME_OID, volumeDataPoint.getEntityOid());
    }

    /**
     * Test sending upload request.
     */
    @Test
    public void testSendUploadRequest() {
        setupCloudBillingData();
        billedCostUploader.uploadBilledCost(topologyInfo, stitchingContext, cloudEntitiesMap);
        Mockito.verify(billedCostUploadServiceSpy, Mockito.times(1))
                .uploadBilledCost(any(StreamObserver.class));
    }

    /**
     * Test that when CloudBillingData is large compared with maximumUploadBilledCostRequestByteSize,
     * separate requests will be created.
     */
    @Test
    public void testCloudBillingDataLargeSize() {
        setupCloudBillingDataLargeSize();
        billedCostUploader.uploadBilledCost(topologyInfo, stitchingContext, cloudEntitiesMap);
        assertTrue(billedCostUploadServiceSpy.requestCount > 1);
    }

    /**
     * Test sending CloudBillingDataPoints without tag, no error or exception.
     */
    @Test
    public void testCloudBillingDataPointsNoTag() {
        final List<CloudBillingData> cloudBillingDataList = Lists.newArrayList(CloudBillingData.newBuilder()
                .setBillingIdentifier(BILLING_IDENTIFIER)
                .addCloudCostBuckets(CloudBillingBucket.newBuilder()
                        .addAllSamples(Lists.newArrayList(CloudBillingDataPoint.newBuilder()
                                .setAccountId(ACCOUNT_LOCAL_ID)
                                .setCloudServiceId(CLOUD_SERVICE_LOCAL_ID).build()))
                        .build())
                .build());
        billedCostUploader.recordCloudBillingData(1L, cloudBillingDataList);
        billedCostUploader.uploadBilledCost(topologyInfo, stitchingContext, cloudEntitiesMap);
        assertTrue(billedCostUploadServiceSpy.requestCount > 0);
    }

    private StitchingContext setupStitchingContext() {
        final Map<String, StitchingEntityData> localIdToEntityMap = ImmutableMap.<String, StitchingEntityData>builder()
                .put(VM_LOCAL_ID, vm)
                .put(DB_LOCAL_ID, db)
                .put(VOLUME_LOCAL_ID, volume)
                .put(ACCOUNT_LOCAL_ID, account)
                .put(REGION_LOCAL_ID, region)
                .put(CLOUD_SERVICE_LOCAL_ID, cloudService).build();
        final TargetStore targetStore = mock(TargetStore.class);
        when(targetStore.getAll()).thenReturn(Collections.emptyList());

        StitchingContext.Builder stitchingContextBuilder = StitchingContext.newBuilder(3, targetStore)
                .setIdentityProvider(mock(IdentityProviderImpl.class));
        stitchingContextBuilder.addEntity(vm, localIdToEntityMap);
        stitchingContextBuilder.addEntity(db, localIdToEntityMap);
        stitchingContextBuilder.addEntity(volume, localIdToEntityMap);
        stitchingContextBuilder.addEntity(account, localIdToEntityMap);
        stitchingContextBuilder.addEntity(region, localIdToEntityMap);
        stitchingContextBuilder.addEntity(cloudService, localIdToEntityMap);
        return stitchingContextBuilder.build();
    }

    private void setupCloudBillingData() {
        final CloudBillingBucket cloudBillingBucket1 = CloudBillingBucket.newBuilder()
                .setTimestampUtcMillis(TIMESTAMP1)
                .addSamples(CloudBillingDataPoint.newBuilder()
                        .setAccountId(ACCOUNT_LOCAL_ID)
                        .setCloudServiceId(CLOUD_SERVICE_LOCAL_ID)
                        .setRegionId(REGION_LOCAL_ID)
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setEntityId(VM_BILLING_ID)
                        .setCostTagGroupId(1)
                        .build())
                .setGranularity(Granularity.DAILY)
                .build();
        final CloudBillingDataPoint dbDp = CloudBillingDataPoint.newBuilder()
                .setAccountId(ACCOUNT_LOCAL_ID)
                .setCloudServiceId(CLOUD_SERVICE_LOCAL_ID)
                .setRegionId(REGION_LOCAL_ID)
                .setEntityType(EntityType.DATABASE_VALUE)
                .setEntityId(DB_BILLING_ID)
                .setCostTagGroupId(1)
                .build();
        final CloudBillingDataPoint volDp = CloudBillingDataPoint.newBuilder()
                .setAccountId(ACCOUNT_LOCAL_ID)
                .setCloudServiceId(CLOUD_SERVICE_LOCAL_ID)
                .setRegionId(REGION_LOCAL_ID)
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setEntityId(VOLUME_LOCAL_ID)
                .setCostTagGroupId(2)
                .build();
        final CloudBillingBucket cloudBillingBucket2 = CloudBillingBucket.newBuilder()
                .setTimestampUtcMillis(TIMESTAMP2)
                .addAllSamples(Lists.newArrayList(dbDp, volDp))
                .setGranularity(Granularity.DAILY)
                .build();
        final CostTagGroup costTagGroup1 = CostTagGroup.newBuilder().putTags("owner", "owner1").build();
        final CostTagGroup costTagGroup2 = CostTagGroup.newBuilder().putTags("owner", "owner2").build();
        final Map<Long, CostTagGroup> costTagGroupMap = ImmutableMap.of(1L, costTagGroup1, 2L, costTagGroup2);
        final List<CloudBillingData> cloudBillingDataList = Lists.newArrayList(CloudBillingData.newBuilder()
                .setBillingIdentifier(BILLING_IDENTIFIER)
                .addAllCloudCostBuckets(Lists.newArrayList(cloudBillingBucket1, cloudBillingBucket2))
                .putAllCostTagGroupMap(costTagGroupMap)
                .build());
        billedCostUploader.recordCloudBillingData(1L, cloudBillingDataList);
    }

    private void setupCloudBillingDataLargeSize() {
        final String longTagKey = "this is a long long long tag key";
        final Map<Long, CostTagGroup> costTagGroupMap = new HashMap<>();
        final List<CloudBillingDataPoint> cloudDataPoints = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            costTagGroupMap.put((long)i, CostTagGroup.newBuilder().putTags(longTagKey, longTagKey + i).build());
            cloudDataPoints.add(CloudBillingDataPoint.newBuilder()
                    .setCostTagGroupId(i)
                    .setAccountId(ACCOUNT_LOCAL_ID)
                    .setCloudServiceId(CLOUD_SERVICE_LOCAL_ID)
                    .build());
        }
        final List<CloudBillingData> cloudBillingDataList = Lists.newArrayList(CloudBillingData.newBuilder()
                .setBillingIdentifier(BILLING_IDENTIFIER)
                .addCloudCostBuckets(CloudBillingBucket.newBuilder().addAllSamples(cloudDataPoints).build())
                .putAllCostTagGroupMap(costTagGroupMap)
                .build());
        billedCostUploader.recordCloudBillingData(1L, cloudBillingDataList);
    }

    /**
     * Test service to process upload requests.
     */
    private static class TestBilledCostUploadService extends BilledCostUploadServiceImplBase {
        private int requestCount = 0;

        /**
         * Test processing requests.
         *
         * @param responseObserver responseObserver
         * @return request observer
         */
        @Override
        public StreamObserver<UploadBilledCostRequest> uploadBilledCost(
                io.grpc.stub.StreamObserver<UploadBilledCostResponse> responseObserver) {
            return new StreamObserver<UploadBilledCostRequest>() {
                @Override
                public void onNext(UploadBilledCostRequest uploadBilledCostRequest) {
                    requestCount++;
                }

                @Override
                public void onError(Throwable throwable) {}

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }
    }
}
