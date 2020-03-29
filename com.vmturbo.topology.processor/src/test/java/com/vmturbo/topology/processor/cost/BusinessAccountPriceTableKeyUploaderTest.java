package com.vmturbo.topology.processor.cost;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.collections.Sets;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.cost.Pricing.BusinessAccountPriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.UploadAccountPriceTableKeyRequest;
import com.vmturbo.common.protobuf.cost.PricingMoles.PricingServiceMole;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.targets.TargetStore;

public class BusinessAccountPriceTableKeyUploaderTest {
    private static final Logger logger = LogManager.getLogger();
    private PricingServiceMole pricingServiceMole = spy(new PricingServiceMole());

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(pricingServiceMole);

    private BusinessAccountPriceTableKeyUploader businessAccountPriceTableKeyUploader;

    private StitchingContext mockStitchingContext = Mockito.mock(StitchingContext.class);
    private TargetStore targetStore = Mockito.mock(TargetStore.class);
    private static final long TARGET_ID_AWS_DISCOVERY_1 = 1;
    private static final long TARGET_ID_AWS_BILLING_1 = 2;
    private final TopologyStitchingEntity mockEntity = mock(TopologyStitchingEntity.class);
    private final Map<Long, SDKProbeType> probeTypeMap = ImmutableMap.of(1L, SDKProbeType.AZURE_COST,
            2L, SDKProbeType.AZURE_SERVICE_PRINCIPAL);
    private final StitchingEntity serviceProvider = Mockito.mock(StitchingEntity.class);

    @Before
    public void setup() {
        // test cost component client
        when(targetStore.getProbeTypeForTarget(TARGET_ID_AWS_DISCOVERY_1)).thenReturn(Optional.of(SDKProbeType.AWS));
        when(targetStore.getProbeTypeForTarget(TARGET_ID_AWS_BILLING_1)).thenReturn(Optional.of(SDKProbeType.AWS_BILLING));
        final PricingServiceBlockingStub priceServiceClient = PricingServiceGrpc.newBlockingStub(server.getChannel());
        businessAccountPriceTableKeyUploader = new BusinessAccountPriceTableKeyUploader(priceServiceClient, targetStore);
        long now = System.currentTimeMillis();
        when(mockStitchingContext.getEntitiesByEntityTypeAndTarget()).thenReturn(
                ImmutableMap.of(
                        EntityType.VIRTUAL_MACHINE,
                        ImmutableMap.of(
                                1234L, Arrays.asList(mockEntity, mockEntity, mockEntity),
                                5678L, Collections.singletonList(mockEntity)),
                        EntityType.PHYSICAL_MACHINE,
                        ImmutableMap.of(
                                1234L, Arrays.asList(mockEntity, mockEntity),
                                5678L, Collections.singletonList(mockEntity)),
                        EntityType.STORAGE,
                        ImmutableMap.of(
                                1234L, Arrays.asList(mockEntity, mockEntity),
                                5678L, Collections.emptyList()),
                        EntityType.SERVICE_PROVIDER,
                        ImmutableMap.of(
                                1234L, Arrays.asList(mockEntity, mockEntity),
                                5678L, Collections.emptyList())
                ));
        List<TopologyStitchingEntity> businessAccounts = Arrays.asList(new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                EntityDTO.newBuilder()
                        .setEntityType(EntityType.BUSINESS_ACCOUNT)
                        .setId("account-1"))
                        .oid(11)
                        .targetId(TARGET_ID_AWS_DISCOVERY_1)
                        .lastUpdatedTime(now)
                        .build()),
                new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                        EntityDTO.newBuilder()
                                .setEntityType(EntityType.BUSINESS_ACCOUNT)
                                .setId("account-2"))
                        .oid(12)
                        .targetId(TARGET_ID_AWS_DISCOVERY_1)
                        .lastUpdatedTime(now)
                        .build()),
                new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                        EntityDTO.newBuilder()
                                .setEntityType(EntityType.BUSINESS_ACCOUNT)
                                .setId("account-1"))
                        .oid(11)
                        .targetId(TARGET_ID_AWS_BILLING_1)
                        .lastUpdatedTime(now)
                        .build()),
                new TopologyStitchingEntity(StitchingEntityData.newBuilder(
                        EntityDTO.newBuilder()
                                .setEntityType(EntityType.BUSINESS_ACCOUNT)
                                .setId("account-2"))
                        .oid(12)
                        .targetId(TARGET_ID_AWS_BILLING_1)
                        .lastUpdatedTime(now)
                        .build()));
        Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.BUSINESS_ACCOUNT)).thenAnswer(
                invocationOnMock -> businessAccounts.stream());
        when(serviceProvider.getEntityType()).thenReturn(EntityType.SERVICE_PROVIDER);
        when(serviceProvider.getLocalId()).thenReturn("ServiceProvider");
        ImmutableMap<ConnectionType, Set<StitchingEntity>> connectedEntityTypeMap = ImmutableMap.of(ConnectionType.AGGREGATED_BY_CONNECTION,
                Sets.newSet(serviceProvider));
        for (TopologyStitchingEntity entity: businessAccounts) {
            entity.addConnectedTo(ConnectionType.AGGREGATED_BY_CONNECTION, Sets.newSet(serviceProvider));
        }
        Mockito.when(mockEntity.getConnectedToByType()).thenReturn(connectedEntityTypeMap);
    }

    @Test
    public void testUploadingBusinessAccount() {
        Map<Long, SDKProbeType> probeTypeMap = ImmutableMap.of(1L, SDKProbeType.AZURE_COST);
        CloudEntitiesMap cloudEntitiesMap = mock(CloudEntitiesMap.class);
        businessAccountPriceTableKeyUploader.uploadAccountPriceTableKeys(mockStitchingContext, probeTypeMap, cloudEntitiesMap);
        Assert.assertEquals(4, mockStitchingContext.getEntitiesOfType(EntityType.BUSINESS_ACCOUNT).count());
        verify(mockStitchingContext, times(1)).getEntitiesByEntityTypeAndTarget();
    }


    @Test
    public void testUploadingBusinessAccountWithValidRoot() {
        CloudEntitiesMap cloudEntitiesMap = mock(CloudEntitiesMap.class);
        when(serviceProvider.getOid()).thenReturn(10L);
        businessAccountPriceTableKeyUploader.uploadAccountPriceTableKeys(mockStitchingContext, probeTypeMap, cloudEntitiesMap);
        Assert.assertEquals(4, mockStitchingContext.getEntitiesOfType(EntityType.BUSINESS_ACCOUNT).count());
        verify(mockStitchingContext, times(1)).getEntitiesByEntityTypeAndTarget();
        PriceTableKey awsPriceTableKey = PriceTableKey.newBuilder()
                .setServiceProviderId(10L).build();
        verify(pricingServiceMole).uploadAccountPriceTableKeys(eq(
                UploadAccountPriceTableKeyRequest.newBuilder(
                        UploadAccountPriceTableKeyRequest.newBuilder()
                                .setBusinessAccountPriceTableKey(BusinessAccountPriceTableKey
                                        .newBuilder().putBusinessAccountPriceTableKey(11L,
                                                awsPriceTableKey)
                                        .putBusinessAccountPriceTableKey(12L,
                                                awsPriceTableKey).build()
                                ).build())
                        .build()));
    }
}
