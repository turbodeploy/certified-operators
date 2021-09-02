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

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import io.grpc.stub.StreamObserver;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.AzureAvailabilitySetInfo;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.AzureScaleSetInfo;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.CoreQuotaInfo.CoreQuotaByBusinessAccount;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.CoreQuotaInfo.CoreQuotaByBusinessAccount.CoreQuotaByRegion;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.CoreQuotaInfo.CoreQuotaByBusinessAccount.CoreQuotaByRegion.CoreQuotaByFamily;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintType;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.UploadActionConstraintInfoRequest;
import com.vmturbo.common.protobuf.action.ActionConstraintDTOMoles.ActionConstraintsServiceMole;
import com.vmturbo.common.protobuf.action.ActionConstraintsServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin.Discovered;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.group.api.ImmutableGroupAndMembers;
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

    private final GroupMemberRetriever groupMemberRetriever = mock(GroupMemberRetriever.class);

    private final SettingPolicyServiceMole testSettingPolicyService =
            spy(new SettingPolicyServiceMole());
    SettingPolicyServiceBlockingStub settingPolicyService = null;

    /**
     * Grpc test server which can be used to mock rpc calls.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(actionConstraintsServiceMole,
            testSettingPolicyService);

    /**
     * Initialization of each test.
     */
    @Before
    public void setup() {
        settingPolicyService = SettingPolicyServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        when(testSettingPolicyService.listSettingPolicies(any()))
                .thenReturn(ImmutableList.of());
        actionConstraintsUploader = new ActionConstraintsUploader(
            entityStore, ActionConstraintsServiceGrpc.newStub(grpcTestServer.getChannel()),
                groupMemberRetriever, settingPolicyService);
        // Pre-populate a few recommend policies, one of which is a recommend only policy.
        when(testSettingPolicyService.listSettingPolicies(any()))
                .thenReturn(ImmutableList.of(
                        createPolicy(StringConstants.AVAILABILITY_SET_RECOMMEND_ONLY_PREFIX
                                + "A recommend only policy", 2116L),
                        createPolicy("An unrelated policy", 2114L)
                ));

    }

    @Nonnull
    private SettingPolicy createPolicy(String policyName, long policyId) {
        return SettingPolicy.newBuilder().setId(policyId).setInfo(SettingPolicyInfo.newBuilder()
                .setName(policyName)
                .setDisplayName(policyName)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)).build();
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
        final String name = StringConstants.CORE_QUOTA_PREFIX + StringConstants.CORE_QUOTA_SEPARATOR
                + "{0}" + StringConstants.CORE_QUOTA_SEPARATOR + "{1}";
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

    private GroupAndMembers makeGroupAndMembers(String name, boolean isDiscovered) {
        Grouping.Builder builder = Grouping.newBuilder()
                .setDefinition(GroupDefinition.newBuilder().setDisplayName(name));
        if (isDiscovered) {
            builder.setOrigin(Origin.newBuilder()
                    .setDiscovered(Discovered.newBuilder()));
        }
        return ImmutableGroupAndMembers.builder()
                .group(builder.build())
                .members(ImmutableList.of())
                .entities(ImmutableList.of())
                .build();
    }

    /**
     * Test uploading Azure ScaleSet information.
     * Build two groups:
     * - "AzureScaleSet::rogueGroup" is a user-defined group that the user named.  This is not a
     *   discovered group and is therefore not a true Azure scale set
     * - "AzureScaleSet::validScaleSet" is a discovered scale set.
     * Only AzureScaleSet::validScaleSet should be uploaded.
     */
    @Test
    public void testBuildAzureScaleSetInfo() {
        List<GroupAndMembers> groupAndMembersList = ImmutableList.of(
                makeGroupAndMembers("AzureScaleSet::rogueGroup", false),
                makeGroupAndMembers("AzureScaleSet::validScaleSet", true)
        );
        when(groupMemberRetriever.getGroupsWithMembers(any())).thenReturn(groupAndMembersList);

        // Start uploading. Use a variable to store upload requests.
        List<UploadActionConstraintInfoRequest> requests = new ArrayList<>();
        final StreamObserver<UploadActionConstraintInfoRequest> requestObserver =
                spy(new StreamObserver<UploadActionConstraintInfoRequest>() {

                    @Override
                    public void onNext(final UploadActionConstraintInfoRequest request) {
                        assertEquals(1, request.getActionConstraintInfoCount());
                        assertEquals(ActionConstraintType.AZURE_SCALE_SET_INFO,
                                request.getActionConstraintInfo(0).getActionConstraintType());
                        requests.add(request);
                    }

                    @Override
                    public void onError(final Throwable throwable) {}

                    @Override
                    public void onCompleted() {}
                });

        actionConstraintsUploader.buildAzureScaleSetInfo(requestObserver, groupMemberRetriever);
        Assert.assertEquals(1, requests.size());
        UploadActionConstraintInfoRequest result = requests.get(0);
        Assert.assertEquals(1, result.getActionConstraintInfoCount());
        ActionConstraintInfo actionConstraintInfo = result.getActionConstraintInfo(0);
        ActionConstraintType constraintType = actionConstraintInfo.getActionConstraintType();
        Assert.assertEquals(ActionConstraintType.AZURE_SCALE_SET_INFO, constraintType);
        Assert.assertTrue(actionConstraintInfo.hasAzureScaleSetInfo());
        AzureScaleSetInfo azureScaleSetInfo = actionConstraintInfo.getAzureScaleSetInfo();
        Assert.assertEquals(1, azureScaleSetInfo.getNamesCount());
        Assert.assertEquals("AzureScaleSet::validScaleSet", azureScaleSetInfo.getNames(0));
    }

    /**
     * Test uploading recommend only constraints.
     */
    @Test
    public void testBuildAzureAvailabilitySetInfo() {
        // Start uploading. Use a variable to store upload requests.
        List<UploadActionConstraintInfoRequest> requests = new ArrayList<>();
        final StreamObserver<UploadActionConstraintInfoRequest> requestObserver =
                spy(new StreamObserver<UploadActionConstraintInfoRequest>() {

                    @Override
                    public void onNext(final UploadActionConstraintInfoRequest request) {
                        assertEquals(1, request.getActionConstraintInfoCount());
                        assertEquals(ActionConstraintType.AZURE_AVAILABILITY_SET_INFO,
                                request.getActionConstraintInfo(0).getActionConstraintType());
                        requests.add(request);
                    }

                    @Override
                    public void onError(final Throwable throwable) {}

                    @Override
                    public void onCompleted() {}
                });

        actionConstraintsUploader.buildAzureAvailabilitySetInfo(requestObserver,
                settingPolicyService);
        Assert.assertEquals(1, requests.size());
        UploadActionConstraintInfoRequest result = requests.get(0);
        Assert.assertEquals(1, result.getActionConstraintInfoCount());
        ActionConstraintInfo actionConstraintInfo = result.getActionConstraintInfo(0);
        ActionConstraintType constraintType = actionConstraintInfo.getActionConstraintType();
        Assert.assertEquals(ActionConstraintType.AZURE_AVAILABILITY_SET_INFO, constraintType);
        Assert.assertTrue(actionConstraintInfo.hasAzureAvailabilitySetInfo());
        AzureAvailabilitySetInfo azureAvailabilitySetInfo =
                actionConstraintInfo.getAzureAvailabilitySetInfo();
        Assert.assertEquals(1, azureAvailabilitySetInfo.getPolicyIdsCount());
        Assert.assertEquals(2116L, azureAvailabilitySetInfo.getPolicyIds(0));
    }
}
