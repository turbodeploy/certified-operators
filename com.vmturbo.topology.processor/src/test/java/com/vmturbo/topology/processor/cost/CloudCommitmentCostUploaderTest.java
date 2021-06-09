package com.vmturbo.topology.processor.cost;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentData;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentData.CloudCommitmentDataBucket;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentData.CloudCommitmentDataBucket.CloudCommitmentDataPoint;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.UploadCloudCommitmentDataRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServicesMoles.CloudCommitmentUploadServiceMole;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentUploadServiceGrpc;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentUploadServiceGrpc.CloudCommitmentUploadServiceBlockingStub;
import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudStatGranularity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.NonMarketEntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.TargetCostData;

/**
 * Unit test class to test the data upload.
 */
public class CloudCommitmentCostUploaderTest {

    private static final long TARGET_ID = 1;
    private static final double CAPACITY = 10;
    private static final double USED = 4;
    private static final long REGION_ID = 2;
    private static final long COMMITMENT_ID = 3;
    private static final long ACCOUNT_ID = 11;
    private static final long SERVICE_PROVIDER_ID = 22;
    private static final String SERVICE = "EC2";
    private static final double DELTA = 0.001d;
    private static final long ENTITY_ID = 33;
    private static final long SERVICE_ID = 44;

    final CloudCommitmentUploadServiceMole uploadServiceMoleSpy = Mockito.spy(new CloudCommitmentUploadServiceMole());
    private CloudCommitmentUploadServiceBlockingStub ccUploadServiceStub;

    /**
     * Set up for all unit tests.
     * @throws IOException IOException thrown by the server start.
     */
    @Before
    public void setup() throws IOException {
        GrpcTestServer grpcServer = GrpcTestServer.newServer(uploadServiceMoleSpy);
        grpcServer.start();
        ccUploadServiceStub = CloudCommitmentUploadServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }

    /**
     * Tests the savings plan data upload.
     */
    @Test
    public void testUploadSavingsPlanUtilization() {


        CloudCommitmentCostUploader uploader = new CloudCommitmentCostUploader(ccUploadServiceStub);
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder().build();
        final CloudEntitiesMap cloudEntitiesMap = createCloudTopologyMap();
        final Map<Long, TargetCostData> costDataByTarget = createCostDataByTarget();

        uploader.uploadCloudCommitmentData(topologyInfo, cloudEntitiesMap, costDataByTarget);

        final ArgumentCaptor<UploadCloudCommitmentDataRequest> uploadRequestCaptor =
                ArgumentCaptor.forClass(UploadCloudCommitmentDataRequest.class);
        Mockito.verify(uploadServiceMoleSpy)
                .uploadCloudCommitmentData(uploadRequestCaptor.capture());

        Assert.assertNotNull(uploadRequestCaptor.getAllValues());
        Assert.assertEquals(1, uploadRequestCaptor.getAllValues().size());
        final UploadCloudCommitmentDataRequest uploadRequest = uploadRequestCaptor.getAllValues().get(0);
        CloudCommitmentData data = uploadRequest.getCloudCommitmentData();
        Assert.assertNotNull(data);
        Assert.assertNotNull(data.getUtilizationDataList());
        Assert.assertEquals(1, data.getUtilizationDataList().size());
        CloudCommitmentDataBucket bucket = data.getUtilizationDataList().get(0);
        CloudStatGranularity granularity = bucket.getGranularity();
        Assert.assertNotNull(granularity);
        Assert.assertEquals(CloudStatGranularity.HOURLY, granularity);
        Assert.assertNotNull(bucket.getSampleCount());
        Assert.assertEquals(1, bucket.getSampleCount());
        CloudCommitmentDataPoint dataPoint = bucket.getSampleList().get(0);
        Assert.assertNotNull(dataPoint);
        CloudCommitmentAmount capacity = dataPoint.getCapacity();
        Assert.assertNotNull(capacity);
        CurrencyAmount capacityAmount = capacity.getAmount();
        Assert.assertNotNull(capacityAmount);
        Assert.assertEquals(CAPACITY, capacityAmount.getAmount(), DELTA);
        CloudCommitmentAmount used = dataPoint.getUsed();
        Assert.assertNotNull(used);
        CurrencyAmount usedAmount = used.getAmount();
        Assert.assertNotNull(usedAmount);
        Assert.assertEquals(USED, usedAmount.getAmount(), DELTA);
        long region = dataPoint.getRegionId();
        Assert.assertEquals(REGION_ID, region);
        long service = dataPoint.getCloudServiceId();
        Assert.assertNotNull(service);
        Assert.assertEquals(SERVICE_ID, service);
        long commitmentOid = dataPoint.getCommitmentId();
        Assert.assertEquals(COMMITMENT_ID, commitmentOid);
        long account = dataPoint.getAccountId();
        Assert.assertEquals(ACCOUNT_ID, account);
        long serviceProviderId = dataPoint.getServiceProviderId();
        Assert.assertEquals(SERVICE_PROVIDER_ID, serviceProviderId);
        long entityId = dataPoint.getEntityId();
        Assert.assertEquals(ENTITY_ID, entityId);

    }

    private Map<Long, TargetCostData> createCostDataByTarget() {

        Map<Long, TargetCostData> costByTargetData = new HashMap<>();
        TargetCostData costData = new TargetCostData();
        costData.targetId = TARGET_ID;
        NonMarketEntityDTO coudCommitmentNME =
                NonMarketEntityDTO.newBuilder()
                        .setEntityType(NonMarketEntityType.CLOUD_COMMITMENT)
                        .setId("1")
                        .setCloudCommitmentData(NonMarketEntityDTO.CloudCommitmentData.newBuilder()
                                .addUtilizationData(NonMarketEntityDTO.CloudCommitmentData.CloudCommitmentDataBucket.newBuilder()
                                        .setGranularity(NonMarketEntityDTO.CloudCommitmentData.CloudCommitmentDataBucket.Granularity.HOURLY)
                                        .addSamples(NonMarketEntityDTO.CloudCommitmentData.CloudCommitmentDataBucket.CloudCommitmentDataPoint.newBuilder()
                                                .setCapacity(NonMarketEntityDTO.CloudCommitmentData.CloudCommitmentDataBucket.CloudCommitmentDataPoint.CloudCommitmentAmount.newBuilder()
                                                        .setAmount(CurrencyAmount.newBuilder().setAmount(CAPACITY)))
                                                .setUsed(NonMarketEntityDTO.CloudCommitmentData.CloudCommitmentDataBucket.CloudCommitmentDataPoint.CloudCommitmentAmount.newBuilder()
                                                        .setAmount(CurrencyAmount.newBuilder().setAmount(USED)))
                                                .setCommitmentId(COMMITMENT_ID + "")
                                                .setRegion(REGION_ID + "")
                                                .setService(SERVICE)
                                                .setAccountId(ACCOUNT_ID + "")
                                                .setEntityId(ENTITY_ID + "")
                                                .setServiceProviderId(SERVICE_PROVIDER_ID + "")
                                                .build())).build()).build();

        costData.cloudCommitmentData = Collections.singletonList(coudCommitmentNME);
        costByTargetData.put(TARGET_ID, costData);
        return costByTargetData;
    }

    private CloudEntitiesMap createCloudTopologyMap() {
        CloudEntitiesMap entityMap =  Mockito.mock(CloudEntitiesMap.class);
        Mockito.when(entityMap.containsKey(REGION_ID + "")).thenReturn(true);
        Mockito.when(entityMap.containsKey(COMMITMENT_ID + "")).thenReturn(true);
        Mockito.when(entityMap.containsKey(SERVICE_PROVIDER_ID + "")).thenReturn(true);
        Mockito.when(entityMap.containsKey(ACCOUNT_ID + "")).thenReturn(true);
        Mockito.when(entityMap.containsKey(ENTITY_ID + "")).thenReturn(true);
        Mockito.when(entityMap.containsKey(SERVICE + "")).thenReturn(true);
        Mockito.when(entityMap.get(REGION_ID + "")).thenReturn(Long.valueOf(REGION_ID));
        Mockito.when(entityMap.get(COMMITMENT_ID + "")).thenReturn(Long.valueOf(COMMITMENT_ID));
        Mockito.when(entityMap.get(SERVICE_PROVIDER_ID + "")).thenReturn(Long.valueOf(SERVICE_PROVIDER_ID));
        Mockito.when(entityMap.get(ACCOUNT_ID + "")).thenReturn(Long.valueOf(ACCOUNT_ID));
        Mockito.when(entityMap.get(ENTITY_ID + "")).thenReturn(Long.valueOf(ENTITY_ID));
        Mockito.when(entityMap.get(SERVICE)).thenReturn(Long.valueOf(SERVICE_ID));
        return  entityMap;
    }
}
