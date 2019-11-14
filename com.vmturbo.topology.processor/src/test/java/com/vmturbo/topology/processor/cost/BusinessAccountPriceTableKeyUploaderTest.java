package com.vmturbo.topology.processor.cost;

import static com.vmturbo.platform.sdk.common.EntityPropertyName.ENROLLMENT_NUMBER;
import static com.vmturbo.platform.sdk.common.EntityPropertyName.OFFER_ID;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.cost.Pricing.BusinessAccountPriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.UploadAccountPriceTableKeyRequest;
import com.vmturbo.common.protobuf.cost.PricingMoles.PricingServiceMole;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.BusinessAccountData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier.PricingIdentifierName;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
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

    @Before
    public void setup() {
        // test cost component client
        when(targetStore.findRootTarget(anyLong())).thenReturn(Optional.of(11L));
        when(targetStore.getProbeTypeForTarget(11L)).thenReturn(Optional.of(SDKProbeType.AWS));
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
                                5678L, Collections.emptyList())
                ));
        Mockito.when(mockStitchingContext.getEntitiesOfType(EntityType.BUSINESS_ACCOUNT)).thenAnswer(
                invocationOnMock -> Stream.of(
                        new TopologyStitchingEntity(StitchingEntityData.newBuilder(
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
                                .build())
                )
        );

    }

    @Test
    public void testUploadingBusinessAccount() {
        Map<Long, SDKProbeType> probeTypeMap = ImmutableMap.of(1L, SDKProbeType.AZURE_COST);
        businessAccountPriceTableKeyUploader.uploadAccountPriceTableKeys(mockStitchingContext, probeTypeMap);
        Assert.assertEquals(4, mockStitchingContext.getEntitiesOfType(EntityType.BUSINESS_ACCOUNT).count());
        verify(mockStitchingContext, times(1)).getEntitiesByEntityTypeAndTarget();
    }


    @Test
    public void testUploadingBusinessAccountWithValidRoot() {
        businessAccountPriceTableKeyUploader.uploadAccountPriceTableKeys(mockStitchingContext, probeTypeMap);
        Assert.assertEquals(4, mockStitchingContext.getEntitiesOfType(EntityType.BUSINESS_ACCOUNT).count());
        verify(mockStitchingContext, times(1)).getEntitiesByEntityTypeAndTarget();

        PriceTableKey awsPriceTableKey = PriceTableKey.newBuilder()
                .setRootProbeType("AWS").build();
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
