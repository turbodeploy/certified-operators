package com.vmturbo.plan.orchestrator.plan;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse.Members;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope.Builder;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.RISetting;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.RISettingsEnum.PreferredTerm;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DemandType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.PaymentOption;

/**
 * Unit tests for {@link PlanRpcServiceUtil}.
 */
public class PlanRpcServiceUtilTest {
    private static final long PLAN_ID = 111L;
    private static final long GROUP_ID = 3001L;
    private static final ReservedInstanceType EXPECTED_RI_TYPE = ReservedInstanceType.newBuilder()
                    .setOfferingClass(ReservedInstanceType.OfferingClass.STANDARD)
                    .setPaymentOption(ReservedInstanceType.PaymentOption.PARTIAL_UPFRONT)
                    .setTermYears(3).build();
    private static final List<Long> BUSINESS_ACCOUNTS_OIDS = Stream.of(10001L, 1002L, 1003L)
                    .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
    private static final List<Long> REGION_OIDS = Stream.of(2001L, 2002L).collect(Collectors
                    .collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
    private static final List<Long> GROUP_OIDS = Stream.of(GROUP_ID).collect(Collectors
                    .collectingAndThen(Collectors.toList(), Collections::unmodifiableList));

    private final GroupServiceMole testGroupRpcService = Mockito.spy(new GroupServiceMole());
    private final RepositoryServiceMole testRepositoryRpcService = Mockito.spy(new RepositoryServiceMole());

    /**
     * gRPC servers.
     */
    @Rule
    public final GrpcTestServer grpcServer = GrpcTestServer.newServer(testGroupRpcService,
                                                                       testRepositoryRpcService);

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
        Assert.assertEquals(EXPECTED_RI_TYPE, request.getPurchaseProfile().getRiType());
        Assert.assertEquals(Collections.emptyList(), request.getRegionsList());
        Assert.assertEquals(BUSINESS_ACCOUNTS_OIDS, request.getAccountsList());
        Assert.assertEquals(DemandType.CONSUMPTION, request.getDemandType());
    }

    private StartBuyRIAnalysisRequest createBuyRIAnalysisRequest(List<Long> scopeOids, String scopeClassName) {
        final RISetting riSetting = RISetting.newBuilder()
                        .setPreferredOfferingClass(OfferingClass.STANDARD)
                        .setPreferredPaymentOption(PaymentOption.PARTIAL_UPFRONT)
                        .setPreferredTerm(PreferredTerm.YEARS_3.getYears())
                        .setDemandType(DemandType.ALLOCATION)
                        .build();
        final Builder planScopeBuilder = PlanScope.newBuilder();
        scopeOids.forEach(oid -> planScopeBuilder.addScopeEntries(PlanScopeEntry
                        .newBuilder().setClassName(scopeClassName)
                        .setScopeObjectOid(oid).build()));

        final ScenarioChange riScenario = ScenarioChange.newBuilder()
                        .setRiSetting(riSetting).build();
        final ScenarioInfo scenarioInfo = ScenarioInfo.newBuilder()
                        .addChanges(getResizeScenarioChanges())
                        .addChanges(riScenario)
                        .setType(StringConstants.OPTIMIZE_CLOUD_PLAN)
                        .setScope(planScopeBuilder.build()).build();
        final StartBuyRIAnalysisRequest request = PlanRpcServiceUtil.createBuyRIRequest(scenarioInfo, riScenario, PLAN_ID,
                                                               GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                                                               RepositoryServiceGrpc.newBlockingStub(grpcServer.getChannel()));
        return request;
    }

    private static ScenarioChange getResizeScenarioChanges() {
        final EnumSettingValue settingValue = EnumSettingValue.newBuilder()
                        .setValue(StringConstants.AUTOMATIC).build();
        final Setting resizeSetting = Setting.newBuilder()
                        .setSettingSpecName(EntitySettingSpecs.Resize.getSettingName())
                        .setEnumSettingValue(settingValue).build();
        return ScenarioChange.newBuilder()
                        .setSettingOverride(SettingOverride.newBuilder().setSetting(resizeSetting).build())
                        .build();
    }

    /**
     * Tests {@link StartBuyRIAnalysisRequest} creation for the optimize cloud plan option 1 with group of regions scope.
     */
    @Test
    public void testCreateBuyRIRequestForScopeWithGroups() {
        GetMembersResponse.Builder memberResponseBuilder = GetMembersResponse.newBuilder();
        PartialEntityBatch.Builder partialEntityBuilder = PartialEntityBatch.newBuilder();
        for (Long oid : REGION_OIDS) {
            memberResponseBuilder.setMembers(Members.newBuilder().addIds(oid));
            partialEntityBuilder.addEntities(PartialEntity.newBuilder()
                            .setMinimal(MinimalEntity.newBuilder()
                                            .setOid(oid)
                                            .setEntityType(EntityType.REGION.getNumber())
                                            .build()));
        }
        final GetMembersResponse membersResponse = memberResponseBuilder.build();
        Mockito.when(testGroupRpcService.getMembers(GetMembersRequest.newBuilder()
                        .setId(GROUP_ID)
                        .setExpandNestedGroups(true)
                        .build())).thenReturn(membersResponse);
        final RetrieveTopologyEntitiesRequest getEntitiesrequest = RetrieveTopologyEntitiesRequest
                        .newBuilder()
                        .addAllEntityOids(membersResponse.getMembers().getIdsList())
                        .setReturnType(PartialEntity.Type.MINIMAL)
                        .build();
        Mockito.when(testRepositoryRpcService.retrieveTopologyEntities(getEntitiesrequest))
                        .thenReturn(Arrays.asList(partialEntityBuilder.build()));
        final StartBuyRIAnalysisRequest request = createBuyRIAnalysisRequest(GROUP_OIDS,
                                                                             StringConstants.GROUP);
        final TopologyInfo topologyInfo = request.getTopologyInfo();
        Assert.assertEquals(PLAN_ID, topologyInfo.getTopologyContextId());
        Assert.assertEquals(StringConstants.OPTIMIZE_CLOUD_PLAN, topologyInfo.getPlanInfo().getPlanType());
        Assert.assertEquals(EXPECTED_RI_TYPE, request.getPurchaseProfile().getRiType());
        Assert.assertEquals(REGION_OIDS, request.getRegionsList());
        Assert.assertEquals(Collections.emptyList(), request.getAccountsList());
        Assert.assertEquals(DemandType.CONSUMPTION, request.getDemandType());
    }
}
