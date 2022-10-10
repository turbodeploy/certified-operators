package com.vmturbo.topology.processor.cost;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import io.grpc.stub.StreamObserver;

import org.junit.Assert;
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
    private static final long TIMESTAMP_3 = 345657L;
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
    private static final String COMPUTE_TIER_NAME = "azure::VM_PROFILE::Standard_B1ls";
    private static final long COMPUTE_TIER_OID = 135L;
    private static final String STORAGE_TIER_NAME = "azure::ST::MANAGED_STANDARD";
    private static final long STORAGE_TIER_OID = 246L;
    private static final long VM_FROM_ID_PROP_VAL_OID = 111L;
    private static final String VM_FROM_ID_PROP_VAL_LOCAL_ID = "vm-1";
    private static final long ACCOUNT_FROM_ID_PROP_VAL_OID = 222L;
    private static final String ACCOUNT_FROM_ID_PROP_VAL_LOCAL_ID = "account-1";
    private static final long REGION_FROM_ID_PROP_VAL_OID = 333L;
    private static final String REGION_FROM_ID_PROP_VAL_LOCAL_ID = "region-1";
    private static final long CLOUD_SERVICE_FROM_ID_PROP_VAL_OID = 444L;
    private static final String CLOUD_SERVICE_FROM_ID_PROP_VAL_LOCAL_ID = "cloud-service-1";
    private static final String VOLUME_LOCAL_ID_2 = "vol-2";
    private static final String ACCOUNT_LOCAL_ID_2 = "account-2";
    private static final String REGION_LOCAL_ID_2 = "region-2";
    private static final String CLOUD_SERVICE_LOCAL_ID_2 = "cloud-service-2";

    private static final String SERVICE_PROVIDER_LOCAL_ID = "SERVICE_PROVIDER::Azure";
    private static final long SERVICE_PROVIDER_OID = 777L;

    private final TestBilledCostUploadService billedCostUploadServiceSpy = spy(new TestBilledCostUploadService());
    /**
     * Test server.
     */
    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(billedCostUploadServiceSpy);

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
    private final StitchingEntityData computeTier = StitchingEntityData.newBuilder(EntityDTO.newBuilder()
            .setEntityType(EntityType.COMPUTE_TIER)
            .setId(COMPUTE_TIER_NAME)).oid(COMPUTE_TIER_OID).build();
    private final StitchingEntityData storageTier = StitchingEntityData.newBuilder(EntityDTO.newBuilder()
            .setEntityType(EntityType.STORAGE_TIER)
            .setId(STORAGE_TIER_NAME)).oid(STORAGE_TIER_OID).build();
    private final StitchingEntityData serviceProvider = StitchingEntityData.newBuilder(EntityDTO.newBuilder()
            .setEntityType(EntityType.SERVICE_PROVIDER)
            .setId(SERVICE_PROVIDER_LOCAL_ID)).oid(SERVICE_PROVIDER_OID).build();

    /**
     * set up.
     */
    @Before
    public void setup() {
        topologyInfo = TopologyInfo.newBuilder().setTopologyId(TOPOLOGY_ID).build();
        stitchingContext = setupStitchingContext();
        cloudEntitiesMap = new CloudEntitiesMap(stitchingContext, new HashMap<>());
        BilledCostUploadServiceStub billUploadServiceClient = BilledCostUploadServiceGrpc.newStub(server.getChannel());
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
        assertEquals(SERVICE_PROVIDER_OID, vmDataPoint.getServiceProviderId());
        assertEquals(REGION_OID, vmDataPoint.getRegionOid());
        assertEquals(VM_OID, vmDataPoint.getEntityOid());
        assertEquals(COMPUTE_TIER_OID, vmDataPoint.getProviderOid());
        assertEquals(EntityType.COMPUTE_TIER_VALUE, vmDataPoint.getProviderType());
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
        assertEquals(STORAGE_TIER_OID, volumeDataPoint.getProviderOid());
        assertEquals(EntityType.STORAGE_TIER_VALUE, volumeDataPoint.getProviderType());
    }

    /**
     * {@link BilledCostDataWrapper#buildBilledCostDataWrappers(StitchingContext, CloudEntitiesMap, Map)} must set OID
     * for entity, account, region and cloud service correctly if the corresponding entity ids are present in the
     * {@link StitchingContext#getTargetEntityLocalIdToOid(long)} map.
     */
    @Test
    public void testBuildBilledCostWrapperOidLookupForEntityIdentityPropertyValues() {
        setupCloudBillingData();
        final List<BilledCostDataWrapper> billedCostDataWrappers = billedCostUploader
            .buildBilledCostDataWrappers(stitchingContext, cloudEntitiesMap, new HashMap<>());
        final BillingDataPoint billingDataPoint = getBillingDataPointFromWrappers(billedCostDataWrappers, TIMESTAMP_3,
            EntityType.VIRTUAL_MACHINE_VALUE);
        Assert.assertNotNull(billingDataPoint);
        Assert.assertEquals(VM_FROM_ID_PROP_VAL_OID, billingDataPoint.getEntityOid());
        Assert.assertEquals(ACCOUNT_FROM_ID_PROP_VAL_OID, billingDataPoint.getAccountOid());
        Assert.assertEquals(REGION_FROM_ID_PROP_VAL_OID, billingDataPoint.getRegionOid());
        Assert.assertEquals(CLOUD_SERVICE_FROM_ID_PROP_VAL_OID, billingDataPoint.getCloudServiceOid());
    }

    /**
     * {@link BilledCostDataWrapper#buildBilledCostDataWrappers(StitchingContext, CloudEntitiesMap, Map)} must set 0L
     * as the OID fields if the corresponding ids are not present in any of the OID sources.
     */
    @Test
    public void testBuildBilledCostWrapperFailedOidLookup() {
        setupCloudBillingData();
        final List<BilledCostDataWrapper> billedCostDataWrappers = billedCostUploader
            .buildBilledCostDataWrappers(stitchingContext, cloudEntitiesMap, new HashMap<>());
        final BillingDataPoint billingDataPoint = getBillingDataPointFromWrappers(billedCostDataWrappers, TIMESTAMP_3,
            EntityType.VIRTUAL_VOLUME_VALUE);
        Assert.assertNotNull(billingDataPoint);
        final long defaultOid = 0L;
        Assert.assertEquals(defaultOid, billingDataPoint.getEntityOid());
        Assert.assertEquals(defaultOid, billingDataPoint.getAccountOid());
        Assert.assertEquals(defaultOid, billingDataPoint.getRegionOid());
        Assert.assertEquals(defaultOid, billingDataPoint.getCloudServiceOid());
    }

    private BillingDataPoint getBillingDataPointFromWrappers(final List<BilledCostDataWrapper> billedCostDataWrappers,
                                                             final long timeStamp, final int entityType) {
        return billedCostDataWrappers.stream()
            .map(wrapper -> wrapper.samplesByCostTagGroupMap)
            .map(Map::values)
            .flatMap(Collection::stream)
            .flatMap(Collection::stream)
            .filter(dp -> dp.getTimestampUtcMillis() == timeStamp)
            .filter(dp -> dp.getEntityType() == entityType)
            .findAny().orElse(null);
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
                .put(COMPUTE_TIER_NAME, computeTier)
                .put(STORAGE_TIER_NAME, storageTier)
                .put(CLOUD_SERVICE_LOCAL_ID, cloudService)
                .put(SERVICE_PROVIDER_LOCAL_ID, serviceProvider).build();
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
        stitchingContextBuilder.addEntity(computeTier, localIdToEntityMap);
        stitchingContextBuilder.addEntity(storageTier, localIdToEntityMap);
        final Map<String, Long> map = ImmutableMap.of(
            VM_FROM_ID_PROP_VAL_LOCAL_ID, VM_FROM_ID_PROP_VAL_OID,
            ACCOUNT_FROM_ID_PROP_VAL_LOCAL_ID, ACCOUNT_FROM_ID_PROP_VAL_OID,
            REGION_FROM_ID_PROP_VAL_LOCAL_ID, REGION_FROM_ID_PROP_VAL_OID,
            CLOUD_SERVICE_FROM_ID_PROP_VAL_LOCAL_ID, CLOUD_SERVICE_FROM_ID_PROP_VAL_OID
        );
        stitchingContextBuilder.setTargetEntityLocalIdToOid(Collections.singletonMap(1L, map));
        stitchingContextBuilder.addEntity(serviceProvider, localIdToEntityMap);
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
                        .setProviderId(COMPUTE_TIER_NAME)
                        .setProviderType(EntityType.COMPUTE_TIER_VALUE)
                        .setServiceProviderId(SERVICE_PROVIDER_LOCAL_ID)
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
                .setServiceProviderId(SERVICE_PROVIDER_LOCAL_ID)
                .build();
        final CloudBillingDataPoint volDp = CloudBillingDataPoint.newBuilder()
                .setAccountId(ACCOUNT_LOCAL_ID)
                .setCloudServiceId(CLOUD_SERVICE_LOCAL_ID)
                .setRegionId(REGION_LOCAL_ID)
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setEntityId(VOLUME_LOCAL_ID)
                .setProviderId(STORAGE_TIER_NAME)
                .setProviderType(EntityType.STORAGE_TIER_VALUE)
                .setCostTagGroupId(2)
                .setServiceProviderId(SERVICE_PROVIDER_LOCAL_ID)
                .build();
        final CloudBillingDataPoint vm2 = CloudBillingDataPoint.newBuilder()
                .setAccountId(ACCOUNT_FROM_ID_PROP_VAL_LOCAL_ID)
                .setCloudServiceId(CLOUD_SERVICE_FROM_ID_PROP_VAL_LOCAL_ID)
                .setRegionId(REGION_FROM_ID_PROP_VAL_LOCAL_ID)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEntityId(VM_FROM_ID_PROP_VAL_LOCAL_ID)
                .setCostTagGroupId(1)
                .setProviderId(COMPUTE_TIER_NAME)
                .setProviderType(EntityType.COMPUTE_TIER_VALUE)
                .build();
        final CloudBillingDataPoint volDp2 = CloudBillingDataPoint.newBuilder()
            .setAccountId(ACCOUNT_LOCAL_ID_2)
            .setCloudServiceId(CLOUD_SERVICE_LOCAL_ID_2)
            .setRegionId(REGION_LOCAL_ID_2)
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .setEntityId(VOLUME_LOCAL_ID_2)
            .setProviderId(STORAGE_TIER_NAME)
            .setProviderType(EntityType.STORAGE_TIER_VALUE)
            .setCostTagGroupId(1)
            .build();
        final CloudBillingBucket cloudBillingBucket2 = CloudBillingBucket.newBuilder()
                .setTimestampUtcMillis(TIMESTAMP2)
                .addAllSamples(Lists.newArrayList(dbDp, volDp))
                .setGranularity(Granularity.DAILY)
                .build();
        final CloudBillingBucket cloudBillingBucket3 = CloudBillingBucket.newBuilder()
            .setTimestampUtcMillis(TIMESTAMP_3)
            .addAllSamples(Arrays.asList(vm2, volDp2))
            .setGranularity(Granularity.DAILY)
            .build();
        final CostTagGroup costTagGroup1 = CostTagGroup.newBuilder().putTags("owner", "owner1").build();
        final CostTagGroup costTagGroup2 = CostTagGroup.newBuilder().putTags("owner", "owner2").build();
        final Map<Long, CostTagGroup> costTagGroupMap = ImmutableMap.of(1L, costTagGroup1, 2L, costTagGroup2);
        final List<CloudBillingData> cloudBillingDataList = Lists.newArrayList(CloudBillingData.newBuilder()
                .setBillingIdentifier(BILLING_IDENTIFIER)
                .addAllCloudCostBuckets(Lists.newArrayList(cloudBillingBucket1, cloudBillingBucket2,
                    cloudBillingBucket3))
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
