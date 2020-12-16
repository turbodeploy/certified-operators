package com.vmturbo.plan.orchestrator.plan;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope.Builder;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.RIProviderSetting;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.RISetting;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration.MigrationReference;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainProto;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.search.CloudType;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.components.common.setting.RISettingsEnum.PreferredTerm;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DemandType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CommonCost.PaymentOption;

/**
 * Unit tests for {@link PlanRpcServiceUtil}.
 */
public class PlanRpcServiceUtilTest {
    private static final long PLAN_ID = 111L;
    private static final long VM_ID_1 = 200L;
    private static final long VM_ID_2 = 201L;
    private static final long REGION_ID_1 = 2001L;
    private static final long REGION_ID_2 = 2002L;
    private static final long GROUP_ID = 3001L;
    private static final ReservedInstanceType EXPECTED_RI_TYPE = ReservedInstanceType.newBuilder()
                    .setOfferingClass(ReservedInstanceType.OfferingClass.STANDARD)
                    .setPaymentOption(PaymentOption.PARTIAL_UPFRONT)
                    .setTermYears(3).build();
    private static final List<Long> BUSINESS_ACCOUNTS_OIDS = Stream.of(10001L, 1002L, 1003L)
                    .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
    private static final List<Long> REGION_OIDS = Stream.of(REGION_ID_1, REGION_ID_2).collect(Collectors
                    .collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
    private static final List<Long> GROUP_OIDS = Stream.of(GROUP_ID).collect(Collectors
                    .collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
    private static final List<Long> VM_OIDS = Stream.of(VM_ID_1, VM_ID_2).collect(Collectors
            .collectingAndThen(Collectors.toList(), Collections::unmodifiableList));

    private final GroupServiceMole testGroupRpcService = Mockito.spy(new GroupServiceMole());
    private final RepositoryServiceMole testRepositoryRpcService = Mockito.spy(new RepositoryServiceMole());
    private final SupplyChainServiceMole testSupplyChainRpcService = Mockito.spy(new SupplyChainServiceMole());

    /**
     * gRPC servers.
     */
    @Rule
    public final GrpcTestServer grpcServer = GrpcTestServer.newServer(
            testGroupRpcService,
            testRepositoryRpcService,
            testSupplyChainRpcService
    );

    /**
     * Tests {@link StartBuyRIAnalysisRequest} creation for the optimize cloud plan option 1 with business accounts scope.
     */
    @Test
    public void testCreateBuyRIRequest() {
        final StartBuyRIAnalysisRequest request = createBuyRIAnalysisRequest(BUSINESS_ACCOUNTS_OIDS,
                                                                           StringConstants.BUSINESS_ACCOUNT);
        final TopologyInfo topologyInfo = request.getTopologyInfo();
        Assert.assertEquals(PLAN_ID, topologyInfo.getTopologyContextId());
        Assert.assertEquals(StringConstants.OPTIMIZE_CLOUD_PLAN, topologyInfo.getPlanInfo().getPlanType());
        Assert.assertEquals(EXPECTED_RI_TYPE, request.getPurchaseProfileByCloudtypeOrThrow(CloudType.AWS.name())
                .getRiType());
        Assert.assertEquals(Collections.emptyList(), request.getRegionsList());
        Assert.assertEquals(BUSINESS_ACCOUNTS_OIDS, request.getAccountsList());
        Assert.assertEquals(DemandType.CONSUMPTION, request.getDemandType());
    }

    private StartBuyRIAnalysisRequest createBuyRIAnalysisRequest(List<Long> scopeOids, String scopeClassName) {
        final RIProviderSetting riProviderSetting = RIProviderSetting.newBuilder()
                .setPreferredOfferingClass(OfferingClass.STANDARD)
                .setPreferredPaymentOption(PaymentOption.PARTIAL_UPFRONT)
                .setPreferredTerm(PreferredTerm.YEARS_3.getYears())
                .build();
        final RISetting riSetting = RISetting.newBuilder()
                .putRiSettingByCloudtype(CloudType.AWS.name(), riProviderSetting)
                        .setDemandType(DemandType.ALLOCATION)
                        .build();
        final Builder planScopeBuilder = PlanScope.newBuilder();
        scopeOids.forEach(oid -> planScopeBuilder.addScopeEntries(PlanScopeEntry
                        .newBuilder().setClassName(scopeClassName)
                        .setScopeObjectOid(oid).build()));

        final ScenarioChange riScenario = ScenarioChange.newBuilder()
                        .setRiSetting(riSetting).build();
        final ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
                        .addChanges(getResizeScenarioChanges(StringConstants.AUTOMATIC))
                        .addChanges(riScenario)
                        .setType(StringConstants.OPTIMIZE_CLOUD_PLAN)
                        .setScope(planScopeBuilder.build()).build();
        final StartBuyRIAnalysisRequest request = PlanRpcServiceUtil.createBuyRIRequest(scenarioInfo, riScenario, PLAN_ID,
                                                               GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                                                               RepositoryServiceGrpc.newBlockingStub(grpcServer.getChannel()));
        return request;
    }

    private ScenarioChange getResizeScenarioChanges(String actionSetting) {
        final EnumSettingValue settingValue = EnumSettingValue.newBuilder()
            .setValue(actionSetting).build();
        final String resizeSettingName = ConfigurableActionSettings.Resize.getSettingName();
        final Setting resizeSetting = Setting.newBuilder().setSettingSpecName(resizeSettingName)
            .setEnumSettingValue(settingValue).build();
        return ScenarioChange.newBuilder()
            .setSettingOverride(SettingOverride.newBuilder()
                .setSetting(resizeSetting).build())
            .build();
    }

    /**
     * Tests {@link StartBuyRIAnalysisRequest} creation for the optimize cloud plan option 1 with group of regions scope.
     */
    @Test
    public void testCreateBuyRIRequestForScopeWithGroups() {
        final GetMembersResponse.Builder memberResponseBuilder =
                GetMembersResponse.newBuilder().setGroupId(GROUP_ID);
        PartialEntityBatch.Builder partialEntityBuilder = PartialEntityBatch.newBuilder();
        for (Long oid : REGION_OIDS) {
            memberResponseBuilder.addMemberId(oid);
            partialEntityBuilder.addEntities(PartialEntity.newBuilder()
                            .setMinimal(MinimalEntity.newBuilder()
                                            .setOid(oid)
                                            .setEntityType(EntityType.REGION.getNumber())
                                            .build()));
        }
        final GetMembersResponse membersResponse = memberResponseBuilder.build();
        Mockito.when(testGroupRpcService.getMembers(GetMembersRequest.newBuilder()
                        .addId(GROUP_ID)
                        .setExpandNestedGroups(true)
                        .build())).thenReturn(Collections.singletonList(membersResponse));
        final RetrieveTopologyEntitiesRequest getEntitiesrequest = RetrieveTopologyEntitiesRequest
                        .newBuilder()
                        .addAllEntityOids(membersResponse.getMemberIdList())
                        .setReturnType(PartialEntity.Type.MINIMAL)
                        .build();
        Mockito.when(testRepositoryRpcService.retrieveTopologyEntities(getEntitiesrequest))
                        .thenReturn(Arrays.asList(partialEntityBuilder.build()));
        final StartBuyRIAnalysisRequest request = createBuyRIAnalysisRequest(GROUP_OIDS,
                                                                             StringConstants.GROUP);
        final TopologyInfo topologyInfo = request.getTopologyInfo();
        Assert.assertEquals(PLAN_ID, topologyInfo.getTopologyContextId());
        Assert.assertEquals(StringConstants.OPTIMIZE_CLOUD_PLAN, topologyInfo.getPlanInfo().getPlanType());
        Assert.assertEquals(EXPECTED_RI_TYPE, request.getPurchaseProfileByCloudtypeOrThrow(CloudType.AWS.name())
                .getRiType());
        Assert.assertEquals(REGION_OIDS, request.getRegionsList());
        Assert.assertEquals(Collections.emptyList(), request.getAccountsList());
        Assert.assertEquals(DemandType.CONSUMPTION, request.getDemandType());
    }

    /**
     * Verify that resizeEnabled is calculated correctly based on ScenarioChanges.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testResizeEnabled() throws Exception {
        PlanInstance planInstance = PlanInstance.newBuilder().setPlanId(1L).setStatus(PlanStatus.QUEUED)
            .setScenario(Scenario.newBuilder().setScenarioInfo(ScenarioInfo.newBuilder()
                .setType(StringConstants.OPTIMIZE_CLOUD_PLAN)
                .addChanges(getResizeScenarioChanges(StringConstants.AUTOMATIC))
                .addChanges(ScenarioChange.newBuilder()
                    .setRiSetting(RISetting.newBuilder().build())))).build();
        Assert.assertTrue(PlanRpcServiceUtil.isScalingEnabled(planInstance.getScenario().getScenarioInfo()));
    }

    /**
     * Verify that resizeEnabled is calculated correctly based on ScenarioChanges.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testResizeDisabled() throws Exception {
        PlanInstance planInstance = PlanInstance.newBuilder().setPlanId(3L).setStatus(PlanStatus.QUEUED)
            .setScenario(Scenario.newBuilder().setScenarioInfo(ScenarioInfo.newBuilder()
                .setType(StringConstants.OPTIMIZE_CLOUD_PLAN)
                .addChanges(getResizeScenarioChanges(StringConstants.DISABLED))
                .addChanges(ScenarioChange.newBuilder()
                    .setRiSetting(RISetting.newBuilder().build())))).build();
        Assert.assertTrue(!PlanRpcServiceUtil.isScalingEnabled(planInstance.getScenario().getScenarioInfo()));
    }

    /**
     * Verify that the method getRegionsBySourceMigration will extract the Region given a single VM.
     */
    @Test
    public void testGetRegionsBySourceMigrationVM() {
        PlanInstance planInstance = createCloudMigrationPlanInstance(ImmutableSet.of(VM_ID_1),
                EntityType.VIRTUAL_MACHINE, null);

        mockSupplyChainRpcGetMultiSupplyChains(ApiEntityType.REGION.typeNumber(), ImmutableSet.of(REGION_ID_1));

        Set<Long> regionIds = PlanRpcServiceUtil.getRegionsBySourceMigration(planInstance,
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel()));

        Assert.assertEquals(1, regionIds.size());
        Assert.assertTrue(regionIds.contains(REGION_ID_1));
    }

    /**
     * Verify that the method getRegionsBySourceMigration will extract the Regions given multiple VMs.
     */
    @Test
    public void testGetRegionsBySourceMigrationVMs() {
        PlanInstance planInstance = createCloudMigrationPlanInstance(ImmutableSet.of(VM_ID_1, VM_ID_2),
                EntityType.VIRTUAL_MACHINE, null);

        mockSupplyChainRpcGetMultiSupplyChains(ApiEntityType.REGION.typeNumber(), ImmutableSet.of(REGION_ID_1, REGION_ID_2));

        Set<Long> regionIds = PlanRpcServiceUtil.getRegionsBySourceMigration(planInstance,
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel()));

        Assert.assertEquals(2, regionIds.size());
        Assert.assertTrue(regionIds.contains(REGION_ID_1));
        Assert.assertTrue(regionIds.contains(REGION_ID_2));
    }

    /**
     * Verify that the method getRegionsBySourceMigration will extract the Regions given a group of VMs.
     */
    @Test
    public void testGetRegionsBySourceMigrationVMGroup() {
        PlanInstance planInstance = createCloudMigrationPlanInstance(ImmutableSet.of(GROUP_ID), null,
                GroupType.REGULAR);

        mockGroupRpcGetMembers(GROUP_ID, ImmutableSet.of(VM_ID_1, VM_ID_2));

        mockSupplyChainRpcGetMultiSupplyChains(ApiEntityType.REGION.typeNumber(), ImmutableSet.of(REGION_ID_1));

        Set<Long> regionIds = PlanRpcServiceUtil.getRegionsBySourceMigration(planInstance,
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel()));

        Assert.assertEquals(1, regionIds.size());
        Assert.assertTrue(regionIds.contains(REGION_ID_1));
    }

    /**
     * Verify that the method getRegionsBySourceMigration will extract the Regions given a group of VMs.
     */
    @Test
    public void testGetRegionsBySourceMigrationNotVMGroup() {
        PlanInstance planInstance = createCloudMigrationPlanInstance(ImmutableSet.of(GROUP_ID), null,
                GroupType.STORAGE_CLUSTER);

        mockGroupRpcGetMembers(GROUP_ID, ImmutableSet.of(VM_ID_1, VM_ID_2));

        mockSupplyChainRpcGetMultiSupplyChains(ApiEntityType.REGION.typeNumber(), ImmutableSet.of(REGION_ID_1));

        Set<Long> regionIds = PlanRpcServiceUtil.getRegionsBySourceMigration(planInstance,
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                SupplyChainServiceGrpc.newBlockingStub(grpcServer.getChannel()));

        Assert.assertEquals(0, regionIds.size());
    }

    private PlanInstance createCloudMigrationPlanInstance(final Set<Long> sourceOids,
                                                          @Nullable EntityType entityType,
                                                          @Nullable GroupType groupType) {
        Set<MigrationReference> sourceReferences = Sets.newHashSet();
        sourceOids.stream().forEach(sourceOid -> {
            MigrationReference.Builder sourceReference = MigrationReference.newBuilder()
                    .setOid(sourceOid)
                    .setGroupType(GroupType.STORAGE_CLUSTER_VALUE);
            if (entityType != null) {
                sourceReference.setEntityType(entityType.ordinal());
            } else if (groupType != null) {
                sourceReference.setGroupType(groupType.ordinal());
            }
            sourceReferences.add(sourceReference.build());
        });

        PlanInstance planInstance = PlanInstance.newBuilder().setPlanId(3L).setStatus(PlanStatus.QUEUED)
                .setScenario(Scenario.newBuilder().setScenarioInfo(ScenarioInfo.newBuilder()
                        .setType(StringConstants.CLOUD_MIGRATION_PLAN)
                        .addChanges(ScenarioChange.newBuilder()
                                .setTopologyMigration(TopologyMigration.newBuilder()
                                        .addAllSource(sourceReferences)
                                        .setDestinationEntityType(TopologyMigration.DestinationEntityType.VIRTUAL_MACHINE)
                                ))))
                .build();

        return planInstance;
    }

    /**
     * Mock the response of the method GroupServiceGrpc::getMembers.
     *
     * @param groupOid - Oid of the Group to expand.
     * @param membersOids - Oids of the members in the response.
     */
    private void mockGroupRpcGetMembers(long groupOid, final Set<Long> membersOids) {
        when(testGroupRpcService.getMembers(GetMembersRequest.newBuilder()
                .setExpandNestedGroups(true)
                .addId(groupOid)
                .build()))
                .thenReturn(Collections.singletonList(GetMembersResponse.newBuilder()
                        .setGroupId(groupOid)
                        .addAllMemberId(membersOids)
                        .build()));
    }

    /**
     * Mock the response of the method SupplyChainServiceGrpc::getMultiSupplyChains.
     *
     * @param nodeType - Type of the SupplyChain node in the response.
     * @param membersOids - Oids of the members in the response.
     */
    private void mockSupplyChainRpcGetMultiSupplyChains(int nodeType, final Set<Long> membersOids) {
        when(testSupplyChainRpcService.getMultiSupplyChains(any()))
                .thenReturn(Arrays.asList(
                        SupplyChainProto.GetMultiSupplyChainsResponse.newBuilder()
                                .setSupplyChain(SupplyChainProto.SupplyChain.newBuilder()
                                        .addSupplyChainNodes(SupplyChainProto.SupplyChainNode.newBuilder()
                                                .setEntityType(nodeType)
                                                .putAllMembersByState(ImmutableMap.of(
                                                        TopologyDTO.EntityState.POWERED_ON_VALUE,
                                                        SupplyChainProto.SupplyChainNode.MemberList.newBuilder()
                                                                .addAllMemberOids(membersOids)
                                                                .build()))))
                                .build()));
    }
}
