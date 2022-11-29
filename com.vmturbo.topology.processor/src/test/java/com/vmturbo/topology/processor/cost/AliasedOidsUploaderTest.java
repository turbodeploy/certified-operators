package com.vmturbo.topology.processor.cost;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import io.grpc.StatusRuntimeException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.cost.AliasedOidsServiceGrpc;
import com.vmturbo.common.protobuf.cost.AliasedOidsServiceGrpc.AliasedOidsServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.AliasedOidsServices.UploadAliasedOidsRequest;
import com.vmturbo.common.protobuf.cost.AliasedOidsServicesMoles.AliasedOidsServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.mediation.hybrid.cloud.common.PropertyName;
import com.vmturbo.platform.common.builders.EntityBuilders;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineContext;

/**
 * Unit tests for {@link AliasedOidsUploader}.
 */
public class AliasedOidsUploaderTest {

    private final long subscriptionTargetId = 12345L;
    private final long billingTargetId = 56789L;
    private final String dbBillingId = "dbBillingId";
    private final long dbAliasId = 13579L;
    private final long dbEntityOid = 14703L;
    private final String vmBillingId = "vmBillingId";
    private final long vmAliasId = 24680L;
    private final long vmEntityOid = 25814L;
    private long topologyCreationTime;
    private TopologyPipelineContext topologyPipelineContext;
    @Mock
    private StitchingContext stitchingContext;

    private final AliasedOidsServiceMole aliasedOidsServiceSpy = Mockito.spy(
            new AliasedOidsServiceMole());

    /**
     * gRPC server for testing.
     */
    @Rule
    public final GrpcTestServer server = GrpcTestServer.newServer(aliasedOidsServiceSpy);

    @Captor
    private ArgumentCaptor<UploadAliasedOidsRequest> uploadAliasedOidsRequestCaptor;

    private AliasedOidsUploader aliasedOidsUploader;

    /**
     * Set up tests.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        topologyCreationTime = Instant.now().toEpochMilli();
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setCreationTime(topologyCreationTime)
                .setTopologyId(54321L)
                .build();
        topologyPipelineContext = new TopologyPipelineContext(topologyInfo);

        final AliasedOidsServiceBlockingStub aliasedOidsServiceBlockingStub =
                AliasedOidsServiceGrpc.newBlockingStub(server.getChannel());
        aliasedOidsUploader = new AliasedOidsUploader(aliasedOidsServiceBlockingStub);
    }

    /**
     * Test that passing an arbitrary targetId to {@link AliasedOidsUploader#removeTarget(Long)}
     * does not result in an exception.
     */
    @Test
    public void removeTargetWhenIdNotInMap() {
        aliasedOidsUploader.removeTarget(11111L);
    }

    /**
     * Test that
     * {@link AliasedOidsUploader#uploadOidMapping(TopologyPipelineContext, StitchingContext)} does
     * not invoke {@link AliasedOidsServiceBlockingStub} for empty map.
     */
    @Test
    public void uploadOidMappingDoesNotUploadEmptyMap() {
        aliasedOidsUploader.uploadOidMapping(topologyPipelineContext, stitchingContext);

        Mockito.verify(aliasedOidsServiceSpy, Mockito.never()).uploadAliasedOids(Mockito.any());
    }

    /**
     * Test that
     * {@link AliasedOidsUploader#uploadOidMapping(TopologyPipelineContext, StitchingContext)}
     * invokes {@link AliasedOidsServiceBlockingStub} to send an {@link UploadAliasedOidsRequest}
     * for specified targets.
     */
    @Test
    public void uploadOidMappingSendsUploadRequest() {
        final Map<String, Long> targetEntityLocalIdToOid = ImmutableMap.of(dbBillingId, dbAliasId,
                vmBillingId, vmAliasId);
        final Map<EntityType, Map<Long, List<TopologyStitchingEntity>>>
                entitiesByEntityTypeAndTarget = buildEntitiesMapByTypeAndTarget();
        Mockito.when(stitchingContext.getTargetEntityLocalIdToOid(billingTargetId)).thenReturn(
                targetEntityLocalIdToOid);
        Mockito.when(stitchingContext.getEntitiesByEntityTypeAndTarget()).thenReturn(
                entitiesByEntityTypeAndTarget);

        aliasedOidsUploader.addTargetProbeType(subscriptionTargetId, SDKProbeType.AZURE);
        aliasedOidsUploader.addBillingTargetId(billingTargetId, SDKProbeType.AZURE_BILLING);

        aliasedOidsUploader.uploadOidMapping(topologyPipelineContext, stitchingContext);

        Mockito.verify(aliasedOidsServiceSpy).uploadAliasedOids(
                uploadAliasedOidsRequestCaptor.capture());
        final UploadAliasedOidsRequest uploadAliasedOidsRequest =
                uploadAliasedOidsRequestCaptor.getValue();
        Assert.assertEquals(topologyCreationTime, uploadAliasedOidsRequest.getBroadcastTimeUtcMs());
        Assert.assertEquals(2, uploadAliasedOidsRequest.getAliasIdToRealIdCount());
        Assert.assertEquals(dbEntityOid,
                uploadAliasedOidsRequest.getAliasIdToRealIdMap().get(dbAliasId).longValue());
        Assert.assertEquals(vmEntityOid,
                uploadAliasedOidsRequest.getAliasIdToRealIdMap().get(vmAliasId).longValue());
    }

    @Nonnull
    private Map<EntityType, Map<Long, List<TopologyStitchingEntity>>> buildEntitiesMapByTypeAndTarget() {
        final String namespace = "namespace";
        final TopologyStitchingEntity dbEntity = new TopologyStitchingEntity(
                EntityBuilders.database("dbEntityId")
                        .build()
                        .toBuilder()
                        .addEntityProperties(EntityProperty.newBuilder()
                                .setNamespace(namespace)
                                .setName(PropertyName.BILLING_ID)
                                .setValue(dbBillingId)
                                .build()),
                dbEntityOid, subscriptionTargetId, 0L);
        final TopologyStitchingEntity vmEntity = new TopologyStitchingEntity(
                EntityBuilders.virtualMachine("vmEntityId")
                        .build()
                        .toBuilder()
                        .addEntityProperties(EntityProperty.newBuilder()
                                .setNamespace(namespace)
                                .setName(PropertyName.BILLING_ID)
                                .setValue(vmBillingId)
                                .build()),
                vmEntityOid, subscriptionTargetId, 0L);
        return ImmutableMap.of(
                EntityType.DATABASE, ImmutableMap.of(
                        subscriptionTargetId, Collections.singletonList(dbEntity)),
                EntityType.VIRTUAL_MACHINE, ImmutableMap.of(
                        subscriptionTargetId, Collections.singletonList(vmEntity)));
    }

    /**
     * Test that
     * {@link AliasedOidsUploader#uploadOidMapping(TopologyPipelineContext, StitchingContext)}
     * does not send a mapping when no alias OID is found for a given {@code billingId}.
     */
    @Test
    public void uploadOidMappingSkipsBillingIdWithoutAliasOid() {
        final Map<String, Long> targetEntityLocalIdToOid = ImmutableMap.of(dbBillingId, dbAliasId);
        final Map<EntityType, Map<Long, List<TopologyStitchingEntity>>>
                entitiesByEntityTypeAndTarget = buildEntitiesMapByTypeAndTarget();
        Mockito.when(stitchingContext.getTargetEntityLocalIdToOid(billingTargetId)).thenReturn(
                targetEntityLocalIdToOid);
        Mockito.when(stitchingContext.getEntitiesByEntityTypeAndTarget()).thenReturn(
                entitiesByEntityTypeAndTarget);

        aliasedOidsUploader.addTargetProbeType(subscriptionTargetId, SDKProbeType.AZURE);
        aliasedOidsUploader.addBillingTargetId(billingTargetId, SDKProbeType.AZURE_BILLING);

        aliasedOidsUploader.uploadOidMapping(topologyPipelineContext, stitchingContext);

        Mockito.verify(aliasedOidsServiceSpy).uploadAliasedOids(
                uploadAliasedOidsRequestCaptor.capture());
        final UploadAliasedOidsRequest uploadAliasedOidsRequest =
                uploadAliasedOidsRequestCaptor.getValue();
        Assert.assertEquals(topologyCreationTime, uploadAliasedOidsRequest.getBroadcastTimeUtcMs());
        Assert.assertEquals(1, uploadAliasedOidsRequest.getAliasIdToRealIdCount());
        Assert.assertEquals(dbEntityOid,
                uploadAliasedOidsRequest.getAliasIdToRealIdMap().get(dbAliasId).longValue());
        Assert.assertFalse(
                uploadAliasedOidsRequest.getAliasIdToRealIdMap().containsKey(vmAliasId));
    }

    /**
     * Test that
     * {@link AliasedOidsUploader#uploadOidMapping(TopologyPipelineContext, StitchingContext)}
     * throws an exception when RPC call produces an exception.
     */
    @Test
    public void uploadOidMappingLogsExceptionFromAliasedOidService() {
        final Map<String, Long> targetEntityLocalIdToOid = ImmutableMap.of(dbBillingId, dbAliasId,
                vmBillingId, vmAliasId);
        final Map<EntityType, Map<Long, List<TopologyStitchingEntity>>>
                entitiesByEntityTypeAndTarget = buildEntitiesMapByTypeAndTarget();
        Mockito.when(stitchingContext.getTargetEntityLocalIdToOid(billingTargetId)).thenReturn(
                targetEntityLocalIdToOid);
        Mockito.when(stitchingContext.getEntitiesByEntityTypeAndTarget()).thenReturn(
                entitiesByEntityTypeAndTarget);
        Mockito.when(aliasedOidsServiceSpy.uploadAliasedOids(Mockito.any())).thenThrow(
                new RuntimeException("Something went wrong"));

        aliasedOidsUploader.addTargetProbeType(subscriptionTargetId, SDKProbeType.AZURE);
        aliasedOidsUploader.addBillingTargetId(billingTargetId, SDKProbeType.AZURE_BILLING);

        Assert.assertThrows(StatusRuntimeException.class,
                () -> aliasedOidsUploader.uploadOidMapping(topologyPipelineContext,
                        stitchingContext));
    }
}
