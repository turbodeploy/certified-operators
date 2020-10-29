package com.vmturbo.topology.processor.actions;

import static com.vmturbo.topology.processor.actions.ActionConstraintsUploader.NUMBER_OF_BUSINESS_ACCOUNT_PER_CHUNK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.CoreQuotaInfo.CoreQuotaByBusinessAccount;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.CoreQuotaInfo.CoreQuotaByBusinessAccount.CoreQuotaByRegion;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.CoreQuotaInfo.CoreQuotaByBusinessAccount.CoreQuotaByRegion.CoreQuotaByFamily;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintType;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.UploadActionConstraintInfoRequest;
import com.vmturbo.common.protobuf.action.ActionConstraintDTOMoles.ActionConstraintsServiceMole;
import com.vmturbo.common.protobuf.action.ActionConstraintsServiceGrpc;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Tests for uploading action constraints to the action orchestrator component.
 */
public class ActionConstraintsUploaderTest {

    private ActionConstraintsUploader actionConstraintsUploader;

    private final EntityStore entityStore = mock(EntityStore.class);

    private final ActionConstraintsServiceMole actionConstraintsServiceMole =
        spy(new ActionConstraintsServiceMole());

    /**
     * Grpc test server which can be used to mock rpc calls.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(actionConstraintsServiceMole);

    /**
     * Initialization of each test.
     */
    @Before
    public void setup() {
        actionConstraintsUploader = new ActionConstraintsUploader(
            entityStore, ActionConstraintsServiceGrpc.newStub(grpcTestServer.getChannel()));
    }

    /**
     * Test {@link ActionConstraintsUploader#uploadActionConstraintInfo(StitchingContext)}.
     */
    @Test
    public void testUploadActionConstraintInfo() {
        final TargetStore targetStore = mock(TargetStore.class);
        when(targetStore.getAll()).thenReturn(Collections.emptyList());

        StitchingContext stitchingContext = StitchingContext.newBuilder(0, targetStore)
            .setIdentityProvider(mock(IdentityProviderImpl.class)).build();
        actionConstraintsUploader.uploadActionConstraintInfo(stitchingContext);
        // ActionConstraintsServiceStub#uploadActionConstraintInfo is an asynchronous method.
        // Because of asynchronization, we want to make sure that the verification of this method
        // happens after it's actually invoked.
        verify(actionConstraintsServiceMole, timeout(5000))
            .uploadActionConstraintInfo(any((StreamObserver.class)));
    }

    /**
     * Test {@link ActionConstraintsUploader#buildCoreQuotaInfo(StitchingContext, StreamObserver)}.
     */
    @Test
    public void testBuildAzureCoreQuotaInfo() {
        // Create a region TopologyStitchingEntity.
        final String nameSpace = "default";
        final String name = StringConstants.CORE_QUOTA_PREFIX + StringConstants.CORE_QUOTA_SEPARATOR +
            "{0}" + StringConstants.CORE_QUOTA_SEPARATOR + "{1}";
        final String value = "10";
        final String subscriptionId = "subscriptionId";
        final long businessAccountId = 100;
        final String family = "family";
        final int numOfSubscriptionIds = 35;
        final int numOfFamilies = 5;
        final long targetId = 10;
        final long regionId1 = 1001;
        final long regionId2 = 1002;

        // Create a list of entity properties of region.
        final List<EntityProperty> entityProperties = new ArrayList<>();
        for (int i = 0; i < numOfSubscriptionIds; i++) {
            for (int j = 0; j < numOfFamilies; j++) {
                entityProperties.add(EntityProperty.newBuilder()
                    .setNamespace(nameSpace)
                    .setName(MessageFormat.format(name, subscriptionId + i, family + j))
                    .setValue(value).build());
            }
            entityProperties.add(EntityProperty.newBuilder()
                .setNamespace(nameSpace)
                .setName(MessageFormat.format(name, subscriptionId + i, StringConstants.TOTAL_CORE_QUOTA))
                .setValue(value).build());
        }

        // Add a property which is not Azure core quota. This will not be uploaded.
        entityProperties.add(EntityProperty.newBuilder()
            .setNamespace(nameSpace).setName("name").setValue("value").build());

        final TopologyStitchingEntity region1 = new TopologyStitchingEntity(StitchingEntityData.newBuilder(
            EntityDTO.newBuilder().setEntityType(EntityType.REGION).setId(String.valueOf(regionId1))
                .addAllEntityProperties(entityProperties)).oid(regionId1).targetId(targetId).build());

        final TopologyStitchingEntity region2 = new TopologyStitchingEntity(StitchingEntityData.newBuilder(
            EntityDTO.newBuilder().setEntityType(EntityType.REGION).setId(String.valueOf(regionId2))
                .addAllEntityProperties(entityProperties)).oid(regionId2).targetId(targetId).build());

        final StitchingContext stitchingContext = mock(StitchingContext.class);
        when(stitchingContext.getEntitiesOfType(EntityType.REGION)).thenAnswer(
            invocationOnMock -> Stream.of(region1, region2));

        final Map<String, Long> localIdToEntityId = new HashMap<>();
        for (int i = 0; i < numOfSubscriptionIds; i++) {
            localIdToEntityId.put(subscriptionId + i, businessAccountId + i);
            when(entityStore.getEntityIdByLocalId(subscriptionId + i))
                    .thenReturn(Optional.of(businessAccountId + i));
        }

        // Start uploading. Use a variable to store upload requests.
        List<UploadActionConstraintInfoRequest> requests = new ArrayList<>();
        final StreamObserver<UploadActionConstraintInfoRequest> requestObserver =
            spy(new StreamObserver<UploadActionConstraintInfoRequest>() {

                @Override
                public void onNext(final UploadActionConstraintInfoRequest request) {
                    assertEquals(1, request.getActionConstraintInfoCount());
                    assertEquals(ActionConstraintType.CORE_QUOTA,
                        request.getActionConstraintInfo(0).getActionConstraintType());
                    requests.add(request);
                }

                @Override
                public void onError(final Throwable throwable) {}

                @Override
                public void onCompleted() {}
            });

        actionConstraintsUploader.buildCoreQuotaInfo(stitchingContext, requestObserver);

        // Verify the number of requests.
        verify(requestObserver,
            times(numOfSubscriptionIds / NUMBER_OF_BUSINESS_ACCOUNT_PER_CHUNK + 1)).onNext(any());

        // Verify the content of requests.
        // All requests except the last one contain NUMBER_OF_BUSINESS_ACCOUNT_PER_CHUNK business accounts.
        for (int i = 0; i < requests.size() - 1; i++) {
            assertEquals(NUMBER_OF_BUSINESS_ACCOUNT_PER_CHUNK,
                requests.get(i).getActionConstraintInfo(0)
                    .getCoreQuotaInfo().getCoreQuotaByBusinessAccountCount());
        }
        // The last request contains numOfSubscriptionIds % NUMBER_OF_BUSINESS_ACCOUNT_PER_CHUNK
        // business accounts.
        assertEquals(numOfSubscriptionIds % NUMBER_OF_BUSINESS_ACCOUNT_PER_CHUNK,
            requests.get(requests.size() - 1).getActionConstraintInfo(0)
                .getCoreQuotaInfo().getCoreQuotaByBusinessAccountCount());

        // Merge all requests because the order of business accounts is not certain.
        final List<CoreQuotaByBusinessAccount> actual = new ArrayList<>();
        for (UploadActionConstraintInfoRequest request : requests) {
            actual.addAll(request.getActionConstraintInfo(0).getCoreQuotaInfo()
                .getCoreQuotaByBusinessAccountList());
        }

        // Construct the expected results
        final List<CoreQuotaByBusinessAccount> expect = new ArrayList<>();
        for (int i = 0; i < numOfSubscriptionIds; i++) {
            expect.add(CoreQuotaByBusinessAccount.newBuilder()
                .setBusinessAccountId(
                    localIdToEntityId.get(subscriptionId + i))
                .addCoreQuotaByRegion(CoreQuotaByRegion.newBuilder()
                    .setRegionId(regionId1).setTotalCoreQuota(Integer.valueOf(value))
                    .addAllCoreQuotaByFamily(IntStream.range(0, numOfFamilies).mapToObj(j ->
                        CoreQuotaByFamily.newBuilder().setFamily(family + j)
                            .setQuota(Integer.valueOf(value)).build())
                        .collect(Collectors.toList())))
                .addCoreQuotaByRegion(CoreQuotaByRegion.newBuilder()
                    .setRegionId(regionId2).setTotalCoreQuota(Integer.valueOf(value))
                    .addAllCoreQuotaByFamily(IntStream.range(0, numOfFamilies).mapToObj(j ->
                        CoreQuotaByFamily.newBuilder().setFamily(family + j)
                            .setQuota(Integer.valueOf(value)).build())
                        .collect(Collectors.toList()))).build());
        }

        assertEquals(expect.size(), actual.size());
        assertTrue(actual.containsAll(expect));
    }
}
