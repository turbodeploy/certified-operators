package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.api.component.external.api.mapper.ActionSpecMapper.mapXlActionStateToApi;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMappingContextFactory.ActionSpecMappingContext;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.VirtualVolumeAspectMapper;
import com.vmturbo.api.component.external.api.mapper.converter.CloudSavingsDetailsDtoConverter;
import com.vmturbo.api.component.external.api.mapper.converter.EntityUptimeDtoConverter;
import com.vmturbo.api.component.external.api.service.PoliciesService;
import com.vmturbo.api.component.external.api.service.ReservedInstancesService;
import com.vmturbo.api.component.external.api.util.ApiUtilsTest;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.conversion.entity.CommodityTypeMapping;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.DiscoveredEntityApiDTO;
import com.vmturbo.api.dto.QueryInputApiDTO;
import com.vmturbo.api.dto.RangeInputApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.action.ActionDetailsApiDTO;
import com.vmturbo.api.dto.action.ActionExecutionAuditApiDTO;
import com.vmturbo.api.dto.action.ActionExecutionCharacteristicApiDTO;
import com.vmturbo.api.dto.action.ActionExecutionCharacteristicInputApiDTO;
import com.vmturbo.api.dto.action.ActionScheduleApiDTO;
import com.vmturbo.api.dto.action.CloudProvisionActionDetailsApiDTO;
import com.vmturbo.api.dto.action.CloudResizeActionDetailsApiDTO;
import com.vmturbo.api.dto.action.CloudSuspendActionDetailsApiDTO;
import com.vmturbo.api.dto.action.OnPremResizeActionDetailsApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.reservedinstance.ReservedInstanceApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.enums.ActionCostType;
import com.vmturbo.api.enums.ActionDetailLevel;
import com.vmturbo.api.enums.ActionDisruptiveness;
import com.vmturbo.api.enums.ActionReversibility;
import com.vmturbo.api.enums.ActionState;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.enums.EntityState;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.ExecutorType;
import com.vmturbo.api.enums.PaymentOption;
import com.vmturbo.api.enums.Platform;
import com.vmturbo.api.enums.ReservedInstanceType;
import com.vmturbo.api.enums.QueryType;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.auth.api.Pair;
import com.vmturbo.common.api.mappers.EnvironmentTypeMapper;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.Allocate;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails.TierCostDetails;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutorInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ActivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AllocateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.BuyRIExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Efficiency;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.InitialPlacement;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Performance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeactivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation.CommodityNewCapacityEntry;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ScaleExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.action.ActionDTO.ScheduleDetails;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTO.UserDetails;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverage;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.CostMoles;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.EntityUptime.EntityUptimeDTO;
import com.vmturbo.common.protobuf.cost.RIBuyContextFetchServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTOMoles;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles.PolicyServiceMole;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoMoles.SupplyChainServiceMole;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * Unit tests for {@link ActionSpecMapper}.
 */
public class ActionSpecMapperTest {

    private static final int POLICY_ID = 10;
    private static final String POLICY_NAME = "policy";
    private static final String ENTITY_TO_RESIZE_NAME = "EntityToResize";
    private static final long REAL_TIME_TOPOLOGY_CONTEXT_ID = 777777L;
    private static final long CONTEXT_ID = 777L;
    private static final long ACTION_LEGACY_INSTANCE_ID = 123L;
    private static final long ACTION_STABLE_IMPACT_ID = 321L;

    private static final ReasonCommodity MEM =
                    createReasonCommodity(CommodityDTO.CommodityType.MEM_VALUE, "grah");
    private static final ReasonCommodity CPU =
                    createReasonCommodity(CommodityDTO.CommodityType.CPU_VALUE, "blah");
    private static final ReasonCommodity VMEM =
                    createReasonCommodity(CommodityDTO.CommodityType.VMEM_VALUE, "foo");
    private static final ReasonCommodity HEAP =
                    createReasonCommodity(CommodityDTO.CommodityType.HEAP_VALUE, "foo");
    private static final ReasonCommodity PROCESSING_UNITS =
            createReasonCommodity(CommodityDTO.CommodityType.PROCESSING_UNITS_VALUE, "foo");

    private static final String TARGET = "Target";
    private static final String SOURCE = "Source";
    private static final String DESTINATION = "Destination";
    private static final String DEFAULT_EXPLANATION = "default explanation";
    private static final String DEFAULT_PRE_REQUISITE_DESCRIPTION = "default pre-requisite description";

    private static final long TARGET_ID = 10L;
    private static final long DATACENTER1_ID = 100L;
    private static final String DC1_NAME = "DC-1";
    private static final long DATACENTER2_ID = 200L;
    private static final String DC2_NAME = "DC-2";
    private static final String TARGET_VENDOR_ID = "qqq";

    private static final long MILLIS_2020_01_01_00_00_00 = 1577854800000L;

    private static final String TIMEZONE_ID = "America/Toronto";
    private static final long SCHEDULE_ID = 505L;
    private static final String SCHEDULE_DISPLAY_NAME = "DailySchedule";
    private static final String ACCEPTING_USER = "administrator";

    private static final String EXTERNAL_NAME = "CHG0030039";
    private static final String EXTERNAL_URL =
        "https://dev77145.service-now.com/change_request.do?sys_id=ee362595db02101093ac84da0b9619d9";

    final CloudSavingsDetailsDtoConverter cloudSavingsDetailsDtoConverter = Mockito.spy(
            new CloudSavingsDetailsDtoConverter(new EntityUptimeDtoConverter()));

    private ActionSpecMapper mapper;
    private ActionSpecMapper mapperWithStableIdEnabled;
    private ActionSpecMapper mapperWithDiscoveredEntityApiDTOEnabled;

    private GroupExpander groupExpander;
    private GroupServiceBlockingStub groupServiceGrpc;

    private PoliciesService policiesService = mock(PoliciesService.class);
    private final ReservedInstancesService reservedInstancesService =
            mock(ReservedInstancesService.class);
    private final UuidMapper uuidMapper = mock(UuidMapper.class);

    private PolicyDTOMoles.PolicyServiceMole policyMole = spy(new PolicyServiceMole());

    private GroupDTOMoles.GroupServiceMole groupMole = spy(new GroupDTOMoles.GroupServiceMole());

    private SupplyChainProtoMoles.SupplyChainServiceMole supplyChainMole =
        spy(new SupplyChainServiceMole());

    private CostMoles.ReservedInstanceUtilizationCoverageServiceMole reservedInstanceUtilizationCoverageServiceMole =
            spy(new CostMoles.ReservedInstanceUtilizationCoverageServiceMole());

    private CostMoles.CostServiceMole costServiceMole = spy(new CostMoles.CostServiceMole());

    private CostMoles.ReservedInstanceBoughtServiceMole reservedInstanceBoughtServiceMole =
            spy(new CostMoles.ReservedInstanceBoughtServiceMole());

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(policyMole, costServiceMole,
            reservedInstanceBoughtServiceMole, reservedInstanceUtilizationCoverageServiceMole,
            groupMole);

    @Rule
    public GrpcTestServer supplyChainGrpcServer = GrpcTestServer.newServer(supplyChainMole);

    private ReservedInstanceMapper reservedInstanceMapper = mock(ReservedInstanceMapper.class);

    private RepositoryApi repositoryApi = mock(RepositoryApi.class);

    private final ServiceEntityMapper serviceEntityMapper = mock(ServiceEntityMapper.class);

    private final VirtualVolumeAspectMapper virtualVolumeAspectMapper = mock(VirtualVolumeAspectMapper.class);

    private final ActionApiInputDTO emptyInputDto = new ActionApiInputDTO();
    private final ApiId scopeWithBuyRiActions = mock(ApiId.class);
    private final Set<ActionDTO.ActionType> buyRiActionTypes = ImmutableSet.of(
            ActionDTO.ActionType.BUY_RI);
    private final Set<Long> buyRiOids = ImmutableSet.of(999L);

    @Before
    public void setup() throws Exception {
        RIBuyContextFetchServiceGrpc.RIBuyContextFetchServiceBlockingStub riBuyContextFetchServiceStub =
                RIBuyContextFetchServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final Policy policy = Policy.newBuilder()
                .setId(POLICY_ID)
                .setPolicyInfo(PolicyInfo.newBuilder()
                        .setName(POLICY_NAME)
                        .setDisplayName(POLICY_NAME))
                .build();
        final List<PolicyResponse> policyResponses = ImmutableList.of(
            PolicyResponse.newBuilder().setPolicy(policy).build());
        Mockito.when(policyMole.getPolicies(any())).thenReturn(policyResponses);
        PolicyApiDTO policyApiDTO = createPolicyApiDTO(POLICY_ID, POLICY_NAME);
        when(policiesService.convertPolicyDTOCollection(Collections.singletonList(policy)))
                .thenReturn(Collections.singletonList(policyApiDTO));
        PolicyServiceGrpc.PolicyServiceBlockingStub policyService =
                PolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());
        groupServiceGrpc = GroupServiceGrpc.newBlockingStub(grpcServer.getChannel());
        groupExpander = new GroupExpander(groupServiceGrpc,
                                          new GroupMemberRetriever(groupServiceGrpc));
        final List<GetMultiSupplyChainsResponse> supplyChainResponses = ImmutableList.of(
            makeGetMultiSupplyChainResponse(1L, DATACENTER1_ID),
            makeGetMultiSupplyChainResponse(2L, DATACENTER2_ID),
            makeGetMultiSupplyChainResponse(3L, DATACENTER2_ID));
        Mockito.when(supplyChainMole.getMultiSupplyChains(any())).thenReturn(supplyChainResponses);
        SupplyChainServiceBlockingStub supplyChainService =
                SupplyChainServiceGrpc.newBlockingStub(supplyChainGrpcServer.getChannel());

        final MultiEntityRequest emptyReq = ApiTestUtils.mockMultiEntityReqEmpty();

        final MultiEntityRequest datacenterReq = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO(DC1_NAME, DATACENTER1_ID, EntityType.DATACENTER_VALUE),
            topologyEntityDTO(DC2_NAME, DATACENTER2_ID, EntityType.DATACENTER_VALUE)));

        when(repositoryApi.entitiesRequest(any())).thenReturn(emptyReq);
        when(repositoryApi.entitiesRequest(Sets.newHashSet(DATACENTER1_ID,
            DATACENTER2_ID)))
            .thenReturn(datacenterReq);

        final SearchRequest emptySearchReq = ApiTestUtils.mockEmptySearchReq();
        when(repositoryApi.getRegion(any())).thenReturn(emptySearchReq);

        final ApiId apiId = mock(ApiId.class);
        when(uuidMapper.fromOid(anyLong()))
                .thenReturn(apiId);
        when(apiId.isPlan())
                .thenReturn(false);

        CostServiceGrpc.CostServiceBlockingStub costServiceBlockingStub =
                CostServiceGrpc.newBlockingStub(grpcServer.getChannel());
        ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub
                reservedInstanceUtilizationCoverageServiceBlockingStub = ReservedInstanceUtilizationCoverageServiceGrpc.newBlockingStub(grpcServer.getChannel());

        when(reservedInstanceUtilizationCoverageServiceMole.getEntityReservedInstanceCoverage(any()))
                .thenReturn(Cost.GetEntityReservedInstanceCoverageResponse.newBuilder()
                            .putCoverageByEntityId(TARGET_ID,
                                    Cost.EntityReservedInstanceCoverage.newBuilder()
                                            .setEntityCouponCapacity(1.0)
                                            .build())
                            .build());
        when(reservedInstanceUtilizationCoverageServiceMole.getProjectedEntityReservedInstanceCoverageStats(any()))
                .thenReturn(Cost.GetProjectedEntityReservedInstanceCoverageResponse.newBuilder()
                        .putCoverageByEntityId(TARGET_ID,
                                Cost.EntityReservedInstanceCoverage.newBuilder()
                                        .setEntityCouponCapacity(8.0)
                                        .build())
                        .build());
        ActionSpecMappingContextFactory actionSpecMappingContextFactory =
                new ActionSpecMappingContextFactory(
                        policyService,
                        Executors.newCachedThreadPool(new ThreadFactoryBuilder().build()),
                        repositoryApi,
                        mock(EntityAspectMapper.class),
                        virtualVolumeAspectMapper,
                        REAL_TIME_TOPOLOGY_CONTEXT_ID,
                        null,
                        null,
                        serviceEntityMapper,
                        supplyChainService,
                        policiesService,
                        reservedInstancesService,
                        groupServiceGrpc,
                        SettingPolicyServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                        mock(SettingsMapper.class));

        final BuyRiScopeHandler buyRiScopeHandler = mock(BuyRiScopeHandler.class);
        when(buyRiScopeHandler.extractActionTypes(emptyInputDto, scopeWithBuyRiActions))
                .thenReturn(buyRiActionTypes);
        when(buyRiScopeHandler.extractBuyRiEntities(scopeWithBuyRiActions))
                .thenReturn(buyRiOids);

        when(virtualVolumeAspectMapper.mapVirtualMachines(anySetOf(Long.class), anyLong())).thenReturn(Collections.emptyMap());
        when(virtualVolumeAspectMapper.mapVirtualVolumes(anySetOf(Long.class), anyLong(), anyBoolean())).thenReturn(new HashMap<>());

        mapper = new ActionSpecMapper(
            actionSpecMappingContextFactory,
            reservedInstanceMapper,
            riBuyContextFetchServiceStub,
            costServiceBlockingStub,
            reservedInstanceUtilizationCoverageServiceBlockingStub,
            buyRiScopeHandler,
            REAL_TIME_TOPOLOGY_CONTEXT_ID,
            uuidMapper,
            cloudSavingsDetailsDtoConverter,
            groupExpander,
            false);
        mapperWithStableIdEnabled = new ActionSpecMapper(
            actionSpecMappingContextFactory,
            reservedInstanceMapper,
            riBuyContextFetchServiceStub,
            costServiceBlockingStub,
            reservedInstanceUtilizationCoverageServiceBlockingStub,
            buyRiScopeHandler,
            REAL_TIME_TOPOLOGY_CONTEXT_ID,
            uuidMapper,
            cloudSavingsDetailsDtoConverter,
            groupExpander,
            true);
        mapperWithDiscoveredEntityApiDTOEnabled = new ActionSpecMapper(
            actionSpecMappingContextFactory,
            reservedInstanceMapper,
            riBuyContextFetchServiceStub,
            costServiceBlockingStub,
            reservedInstanceUtilizationCoverageServiceBlockingStub,
            buyRiScopeHandler,
            REAL_TIME_TOPOLOGY_CONTEXT_ID,
            uuidMapper,
            cloudSavingsDetailsDtoConverter,
            groupExpander,
            true);
    }

    /**
     * Should translate a move action from action orchestartor to ActionApiDTO.
     *
     * <p>The external name and URL are not set because the action is not externally approved. As a
     * result, there should be no errors and the ActionApiDTO should also have null external name and
     * url.</p>
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testMapMove() throws Exception {
        ActionInfo moveInfo = getHostMoveActionInfo();
        Explanation compliance = Explanation.newBuilder()
            .setMove(MoveExplanation.newBuilder()
                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                    .setCompliance(Compliance.newBuilder()
                        .addMissingCommodities(MEM)
                        .addMissingCommodities(CPU)
                        .build())
                    .build())
                .build())
            .build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, compliance), REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("3", actionApiDTO.getTarget().getUuid());

        ActionApiDTO first = actionApiDTO.getCompoundActions().get(0);
        assertEquals(SOURCE, first.getCurrentEntity().getDisplayName());
        assertEquals("1", first.getCurrentValue());

        assertEquals(DESTINATION, first.getNewEntity().getDisplayName());
        assertEquals("2", first.getNewValue());

        assertEquals(ActionType.MOVE, actionApiDTO.getActionType());
        assertEquals("default explanation", actionApiDTO.getRisk().getDescription());
        assertEquals(ImmutableSet.of("Mem", "CPU"), actionApiDTO.getRisk().getReasonCommodities());

        // Validate that the importance value is 0
        assertEquals(0, actionApiDTO.getImportance(), 0.05);

        // no external url
        assertEquals(null, actionApiDTO.getExternalActionName());
        assertEquals(null, actionApiDTO.getExternalActionName());
    }

    /**
     * Checks that Move Virtual Volume action will be converted into {@link ActionApiDTO} including resources
     * information that effectively points to related volume instances.
     *
     * @throws Exception in case of error while action mapping process.
     */
    @Test
    public void testMapMoveWithResources() throws Exception {
        final Collection<Long> resourceIds = ImmutableSet.of(4L, 5L);
        final ActionInfo moveInfo = getMoveActionInfoWithResources(ApiEntityType.STORAGE.apiStr(),
                resourceIds, true);
        final Explanation compliance = Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setCompliance(Compliance.newBuilder()
                                        .addMissingCommodities(MEM)
                                        .addMissingCommodities(CPU)
                                        .build())
                                .build())
                        .build())
                .build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        final ActionApiDTO actionApiDTO =
                mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, compliance),
                        REAL_TIME_TOPOLOGY_CONTEXT_ID);
        final Collection<BaseApiDTO> resources = actionApiDTO.getTarget().getConnectedEntities();
        Assert.assertThat(resources.stream().map(BaseApiDTO::getClassName).collect(Collectors.toSet()),
                CoreMatchers.is(Collections.singleton(ApiEntityType.VIRTUAL_VOLUME.apiStr())));
        Assert.assertThat(resources.stream().map(BaseApiDTO::getUuid).collect(Collectors.toSet()),
                CoreMatchers.is(resourceIds.stream().map(String::valueOf).collect(Collectors.toSet())));
    }

    /**
     * Test the case of conversion of move action between hosts.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testMapVmMoveAcrossHosts() throws Exception {
        // ARRANGE
        final long vmId = 1001L;
        final long pm1Id = 1002L;
        final long pm2Id = 1003L;
        final long cluster1Id = 2001L;
        final long cluster2Id = 2002L;
        final String cluster1Name = "Cluster1";
        final String cluster2Name = "Cluster2";


        ActionInfo moveInfo = ActionInfo.newBuilder().setMove(Move.newBuilder()
            .setTarget(ActionEntity.newBuilder()
                .setId(vmId)
                .setType(EntityType.VIRTUAL_MACHINE.getNumber())
                .build())
            .addChanges(ChangeProvider.newBuilder()
                    .setSource(ActionEntity.newBuilder()
                        .setId(pm1Id)
                        .setType(EntityType.PHYSICAL_MACHINE.getNumber())
                        .build())
                    .setDestination(ActionEntity.newBuilder()
                        .setId(pm2Id)
                        .setType(EntityType.PHYSICAL_MACHINE.getNumber())
                        .build())
                    .build())
                .build())
            .build();

        final ActionSpec actionSpec = buildActionSpec(moveInfo, Explanation.getDefaultInstance());

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(topologyEntityDTOList(Lists.newArrayList(
            new TestEntity(TARGET, vmId, EntityType.VIRTUAL_MACHINE_VALUE),
            new TestEntity(SOURCE, pm1Id, EntityType.PHYSICAL_MACHINE_VALUE),
            new TestEntity(DESTINATION, pm2Id, EntityType.PHYSICAL_MACHINE_VALUE))));
        when(repositoryApi.entitiesRequest(any()))
            .thenReturn(req);

        ArgumentCaptor<GroupDTO.GetGroupsForEntitiesRequest> paramCaptor =
            ArgumentCaptor.forClass(GroupDTO.GetGroupsForEntitiesRequest.class);
        when(groupMole.getGroupsForEntities(paramCaptor.capture()))
            .thenReturn(GroupDTO.GetGroupsForEntitiesResponse
            .newBuilder()
            .addGroups(GroupDTO.Grouping.newBuilder()
                .setId(cluster1Id)
                .setDefinition(GroupDTO.GroupDefinition.newBuilder()
                    .setDisplayName(cluster1Name)
                    .build())
                .build())
            .addGroups(GroupDTO.Grouping.newBuilder()
                .setId(cluster2Id)
                .setDefinition(GroupDTO.GroupDefinition.newBuilder()
                    .setDisplayName(cluster2Name)
                    .build())
                .build())
            .putEntityGroup(pm1Id, GroupDTO.Groupings.newBuilder().addGroupId(cluster1Id).build())
            .putEntityGroup(pm2Id, GroupDTO.Groupings.newBuilder().addGroupId(cluster2Id).build())
            .build());
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);

        // ACT
        ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID);

        // ASSERT
        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals(String.valueOf(vmId), actionApiDTO.getTarget().getUuid());

        ActionApiDTO first = actionApiDTO.getCompoundActions().get(0);
        assertEquals(SOURCE, first.getCurrentEntity().getDisplayName());
        assertEquals(String.valueOf(pm1Id), first.getCurrentValue());

        assertEquals(DESTINATION, first.getNewEntity().getDisplayName());
        assertEquals(String.valueOf(pm2Id), first.getNewValue());

        // check the request for getting clusters
        final GroupDTO.GetGroupsForEntitiesRequest request = paramCaptor.getValue();
        assertThat(request.getEntityIdList(), containsInAnyOrder(pm1Id, pm2Id));
        assertThat(request.getGroupTypeList(), containsInAnyOrder(
            CommonDTO.GroupDTO.GroupType.COMPUTE_HOST_CLUSTER));
        assertTrue(request.getLoadGroupObjects());

        // check the cluster info on the request
        final List<BaseApiDTO> currentClusterList = first.getCurrentEntity().getConnectedEntities();
        assertThat(currentClusterList.size(), is(1));
        final ServiceEntityApiDTO currentCluster = (ServiceEntityApiDTO)currentClusterList.get(0);
        assertThat(currentCluster.getUuid(), is(String.valueOf(pm1Id)));
        assertThat(currentCluster.getDisplayName(), is(cluster1Name));
        assertThat(currentCluster.getClassName(), is(StringConstants.CLUSTER));

        final List<BaseApiDTO> newClusterList = first.getNewEntity().getConnectedEntities();
        assertThat(newClusterList.size(), is(1));
        final ServiceEntityApiDTO newCluster = (ServiceEntityApiDTO)newClusterList.get(0);
        assertThat(newCluster.getUuid(), is(String.valueOf(pm2Id)));
        assertThat(newCluster.getDisplayName(), is(cluster2Name));
        assertThat(newCluster.getClassName(), is(StringConstants.CLUSTER));

    }

    /**
     * Test that if entity involved in action missed in our topology we get minimal information
     * (id, entityType and environmentType is exist) about it from ActionEntity.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testMapMoveWhenInvolvedTargetsAbsentInTopology() throws Exception {
        final int sourceEntityType = EntityType.PHYSICAL_MACHINE.getNumber();
        final int targetEntityType = EntityType.VIRTUAL_MACHINE.getNumber();
        final int targetEntityId = 3;
        final EnvironmentTypeEnum.EnvironmentType targetEntityEnvType =
                EnvironmentTypeEnum.EnvironmentType.ON_PREM;
        final ActionInfo moveInfo =
                getMoveActionInfoForEntitiesAbsentInTopology(sourceEntityType, targetEntityType,
                        targetEntityId, targetEntityEnvType);
        final Explanation compliance = Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setCompliance(Compliance.newBuilder()
                                        .addMissingCommodities(MEM)
                                        .addMissingCommodities(CPU)
                                        .build())
                                .build())
                        .build())
                .build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        ActionApiDTO actionApiDTO =
                mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, compliance),
                        REAL_TIME_TOPOLOGY_CONTEXT_ID);
        final ServiceEntityApiDTO targetEntity = actionApiDTO.getTarget();
        // targetEntity is null because when we populate it we use only information from
        // ActionEntity (uuid, entityType and environmentType)
        assertNull(targetEntity.getDisplayName());
        assertEquals(ApiEntityType.VIRTUAL_MACHINE.apiStr(), targetEntity.getClassName());
        assertEquals(String.valueOf(targetEntityId), targetEntity.getUuid());
        assertEquals(EnvironmentTypeMapper.fromXLToApi(targetEntityEnvType),
                targetEntity.getEnvironmentType());

        assertEquals(ActionType.MOVE, actionApiDTO.getActionType());
        assertEquals("default explanation", actionApiDTO.getRisk().getDescription());
        assertEquals(ImmutableSet.of("Mem", "CPU"), actionApiDTO.getRisk().getReasonCommodities());
    }

    /**
     * Test mapping of Scale action.
     * @throws Exception in case of mapping error.
     */
    @Test
    public void testMapScale() throws Exception {
        final ActionInfo scaleInfo = getScaleActionInfo();
        final Explanation explanation = Explanation.newBuilder()
            .setScale(ScaleExplanation.newBuilder()
                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                    .setEfficiency(Efficiency.newBuilder()
                        .addUnderUtilizedCommodities(MEM)
                        .addUnderUtilizedCommodities(CPU)))
                .build())
            .build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
                buildActionSpecWithDisruptiveReversible(scaleInfo, explanation, false, true), REAL_TIME_TOPOLOGY_CONTEXT_ID);

        assertEquals(ActionType.SCALE, actionApiDTO.getActionType());
        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("3", actionApiDTO.getTarget().getUuid());
        assertEquals("default explanation", actionApiDTO.getRisk().getDescription());
        assertEquals(ImmutableSet.of("Mem", "CPU"), actionApiDTO.getRisk().getReasonCommodities());

        ActionApiDTO first = actionApiDTO.getCompoundActions().get(0);
        assertEquals(SOURCE, first.getCurrentEntity().getDisplayName());
        assertEquals("1", first.getCurrentValue());
        assertEquals(DESTINATION, first.getNewEntity().getDisplayName());
        assertEquals("2", first.getNewValue());

        // Validate that the importance value is 0
        assertEquals(0, actionApiDTO.getImportance(), 0.05);

        // Validate actionExecutionCharacteristics
        ActionExecutionCharacteristicApiDTO actionExecutionCharacteristics
                = actionApiDTO.getExecutionCharacteristics();
        assertNotNull(actionExecutionCharacteristics);
        assertEquals(ActionDisruptiveness.NON_DISRUPTIVE, actionExecutionCharacteristics.getDisruptiveness());
        assertEquals(ActionReversibility.REVERSIBLE, actionExecutionCharacteristics.getReversibility());
    }

    /**
     * Test mapping of scheduled automated scale.
     *
     * <p>The action spec also sets the external URL and name. Those should be copied to the
     * ActionApiDTO.</p>
     *
     * @throws Exception in case of mapping error.
     */
    @Test
    public void testMapScheduledExternalScaleAutomated() throws Exception {
        final ActionInfo scaleInfo = getScaleActionInfo();
        final Explanation explanation = Explanation.newBuilder()
            .setScale(ScaleExplanation.newBuilder()
                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                    .setEfficiency(Efficiency.newBuilder()))
                .build())
            .build();

        long scheduleStartTimestamp = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(2);
        long scheduleEndTimestamp = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(3);

        ActionSpec.ActionSchedule schedule =
            ActionDTO.ActionSpec.ActionSchedule.newBuilder()
                .setScheduleTimezoneId(TIMEZONE_ID)
                .setScheduleId(SCHEDULE_ID)
                .setScheduleDisplayName(SCHEDULE_DISPLAY_NAME)
                .setExecutionWindowActionMode(ActionMode.AUTOMATIC)
                .setStartTimestamp(scheduleStartTimestamp)
                .setEndTimestamp(scheduleEndTimestamp)
                .build();

        ActionSpec actionSpec = buildActionSpec(scaleInfo, explanation, Optional.empty(),
            Optional.empty(), schedule,
            EXTERNAL_NAME,
            EXTERNAL_URL);
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
            actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID);

        assertNotNull(actionApiDTO.getActionSchedule());
        ActionScheduleApiDTO actionScheduleApiDTO = actionApiDTO.getActionSchedule();
        assertThat(actionScheduleApiDTO.getNextOccurrenceTimestamp(), is(scheduleStartTimestamp));
        assertThat(actionScheduleApiDTO.getNextOccurrence(),
            is(DateTimeUtil.toString(scheduleStartTimestamp, TimeZone.getTimeZone(TIMEZONE_ID))));
        assertThat(actionScheduleApiDTO.getMode(), is(com.vmturbo.api.enums.ActionMode.AUTOMATIC));
        assertThat(actionScheduleApiDTO.getUuid(), is(String.valueOf(SCHEDULE_ID)));
        assertThat(actionScheduleApiDTO.getDisplayName(), is(SCHEDULE_DISPLAY_NAME));
        assertNull(actionScheduleApiDTO.getRemaingTimeActiveInMs());
        assertNull(actionScheduleApiDTO.getUserName());
        assertTrue(actionScheduleApiDTO.isAcceptedByUserForMaintenanceWindow());

        // copy external name and url
        assertEquals(EXTERNAL_NAME, actionApiDTO.getExternalActionName());
        assertEquals(EXTERNAL_URL, actionApiDTO.getExternalActionUrl());
    }

    /**
     * Test mapping of scheduled manual scale action which is accepted by user.
     * @throws Exception in case of mapping error.
     */
    @Test
    public void testMapScheduledScaleManualAccepted() throws Exception {
        final ActionInfo scaleInfo = getScaleActionInfo();
        final Explanation explanation = Explanation.newBuilder()
            .setScale(ScaleExplanation.newBuilder()
                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                    .setEfficiency(Efficiency.newBuilder()))
                .build())
            .build();

        long scheduleStartTimestamp = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(2);
        long scheduleEndTimestamp = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(3);

        ActionSpec.ActionSchedule schedule =
            ActionDTO.ActionSpec.ActionSchedule.newBuilder()
                .setScheduleTimezoneId(TIMEZONE_ID)
                .setScheduleId(SCHEDULE_ID)
                .setScheduleDisplayName(SCHEDULE_DISPLAY_NAME)
                .setExecutionWindowActionMode(ActionMode.MANUAL)
                .setStartTimestamp(scheduleStartTimestamp)
                .setEndTimestamp(scheduleEndTimestamp)
                .setAcceptingUser(ACCEPTING_USER)
                .build();

        ActionSpec actionSpec = buildActionSpec(scaleInfo, explanation, Optional.empty(),
            Optional.empty(), schedule);
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
            actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID);

        assertNotNull(actionApiDTO.getActionSchedule());
        ActionScheduleApiDTO actionScheduleApiDTO = actionApiDTO.getActionSchedule();
        assertThat(actionScheduleApiDTO.getNextOccurrenceTimestamp(), is(scheduleStartTimestamp));
        assertThat(actionScheduleApiDTO.getNextOccurrence(),
            is(DateTimeUtil.toString(scheduleStartTimestamp, TimeZone.getTimeZone(TIMEZONE_ID))));
        assertThat(actionScheduleApiDTO.getMode(), is(com.vmturbo.api.enums.ActionMode.MANUAL));
        assertThat(actionScheduleApiDTO.getUuid(), is(String.valueOf(SCHEDULE_ID)));
        assertThat(actionScheduleApiDTO.getDisplayName(), is(SCHEDULE_DISPLAY_NAME));
        assertNull(actionScheduleApiDTO.getRemaingTimeActiveInMs());
        assertThat(actionScheduleApiDTO.getUserName(), is(ACCEPTING_USER));
        assertTrue(actionScheduleApiDTO.isAcceptedByUserForMaintenanceWindow());
    }

    /**
     * Test mapping of scheduled manual scale action which is in the window.
     * @throws Exception in case of mapping error.
     */
    @Test
    public void testMapScheduledScaleManualInWindow() throws Exception {
        final ActionInfo scaleInfo = getScaleActionInfo();
        final Explanation explanation = Explanation.newBuilder()
            .setScale(ScaleExplanation.newBuilder()
                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                    .setEfficiency(Efficiency.newBuilder()))
                .build())
            .build();

        long scheduleEndTimestamp = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1);

        ActionSpec.ActionSchedule schedule =
            ActionDTO.ActionSpec.ActionSchedule.newBuilder()
                .setScheduleTimezoneId(TIMEZONE_ID)
                .setScheduleId(SCHEDULE_ID)
                .setScheduleDisplayName(SCHEDULE_DISPLAY_NAME)
                .setExecutionWindowActionMode(ActionMode.MANUAL)
                .setEndTimestamp(scheduleEndTimestamp)
                .build();

        ActionSpec actionSpec = buildActionSpec(scaleInfo, explanation, Optional.empty(),
            Optional.empty(), schedule);
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
            actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID);

        assertNotNull(actionApiDTO.getActionSchedule());
        ActionScheduleApiDTO actionScheduleApiDTO = actionApiDTO.getActionSchedule();
        assertNull(actionScheduleApiDTO.getNextOccurrenceTimestamp());
        assertNull(actionScheduleApiDTO.getNextOccurrence());
        assertThat(actionScheduleApiDTO.getMode(), is(com.vmturbo.api.enums.ActionMode.MANUAL));
        assertThat(actionScheduleApiDTO.getUuid(), is(String.valueOf(SCHEDULE_ID)));
        assertThat(actionScheduleApiDTO.getDisplayName(), is(SCHEDULE_DISPLAY_NAME));
        assertTrue(actionScheduleApiDTO.getRemaingTimeActiveInMs() > 0);
        assertNull(actionScheduleApiDTO.getUserName());
        assertFalse(actionScheduleApiDTO.isAcceptedByUserForMaintenanceWindow());
    }

    /**
     * Test mapping of Allocate action.
     * @throws Exception in case of mapping error.
     */
    @Test
    public void testMapAllocate() throws Exception {
        final ActionInfo allocateInfo = getAllocateActionInfo();
        final Explanation explanation = Explanation.newBuilder()
                .setAllocate(AllocateExplanation.getDefaultInstance())
                .build();

        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
                buildActionSpec(allocateInfo, explanation), REAL_TIME_TOPOLOGY_CONTEXT_ID);

        assertEquals(ActionType.ALLOCATE, actionApiDTO.getActionType());
        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("3", actionApiDTO.getTarget().getUuid());
        assertEquals("4", actionApiDTO.getTemplate().getUuid());
        assertEquals("default explanation", actionApiDTO.getRisk().getDescription());
        assertNull(actionApiDTO.getRisk().getReasonCommodities());
        assertEquals(0, actionApiDTO.getImportance(), 0.05);
    }

    @Test
    public void testMapInitialPlacement() throws Exception {
        ActionInfo moveInfo = getHostMoveActionInfo();
        Explanation placement = Explanation.newBuilder()
                        .setMove(MoveExplanation.newBuilder()
                            .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setInitialPlacement(InitialPlacement.newBuilder().build())
                                .build())
                            .build())
                        .build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, placement), REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("3", actionApiDTO.getTarget().getUuid());

        // The compound action should have a "current" value, because it has a source ID
        // in the input.
        ActionApiDTO first = actionApiDTO.getCompoundActions().get(0);
        assertEquals("1", first.getCurrentValue());
        assertEquals(DESTINATION, first.getNewEntity().getDisplayName());
        assertEquals("2", first.getNewValue());

        // The outer/main action should be an initial placement with no source.
        assertEquals(ActionType.START, actionApiDTO.getActionType());
        assertNull(actionApiDTO.getCurrentValue());
        assertEquals("default explanation", actionApiDTO.getRisk().getDescription());
        assertNull(actionApiDTO.getRisk().getReasonCommodities());
    }

    @Test
    public void testMapStorageMove() throws Exception {
        ActionInfo moveInfo = getStorageMoveActionInfo();
        Explanation compliance = Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setCompliance(Compliance.newBuilder()
                                .addMissingCommodities(MEM)
                                .addMissingCommodities(CPU).build())
                        .build())
                    .build())
                .build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, compliance), REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("3", actionApiDTO.getTarget().getUuid());

        assertEquals(1, actionApiDTO.getCompoundActions().size());
        ActionApiDTO first = actionApiDTO.getCompoundActions().get(0);
        assertEquals(SOURCE, first.getCurrentEntity().getDisplayName());
        assertEquals("1", first.getCurrentValue());

        assertEquals(DESTINATION, first.getNewEntity().getDisplayName());
        assertEquals("2", first.getNewValue());

        assertEquals(ActionType.MOVE, actionApiDTO.getActionType());
        assertEquals("default explanation", actionApiDTO.getRisk().getDescription());
        assertEquals(ImmutableSet.of("Mem", "CPU"), actionApiDTO.getRisk().getReasonCommodities());
    }

    /**
     * If move action doesn't have the source entity/id, it's ADD_PROVIDER for Storage.
     *
     * @throws Exception
     */
    @Test
    public void testMapStorageMoveWithoutSourceId() throws Exception {
        ActionInfo moveInfo = getMoveActionInfo(ApiEntityType.STORAGE.apiStr(), false);
        Explanation compliance = Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setCompliance(Compliance.newBuilder()
                                        .addMissingCommodities(MEM)
                                        .addMissingCommodities(CPU).build())
                                .build())
                        .build())
                .build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        ActionApiDTO actionApiDTO =
                mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, compliance), REAL_TIME_TOPOLOGY_CONTEXT_ID);

        verify(repositoryApi).entitiesRequest(Sets.newHashSet(3L, 2L));
        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("3", actionApiDTO.getTarget().getUuid());

        assertEquals(1, actionApiDTO.getCompoundActions().size());
        ActionApiDTO first = actionApiDTO.getCompoundActions().get(0);
        assertEquals(DESTINATION, first.getNewEntity().getDisplayName());
        assertEquals("2", first.getNewValue());

        assertEquals(ActionType.START, actionApiDTO.getActionType());
        assertEquals("default explanation", actionApiDTO.getRisk().getDescription());
        assertEquals(ImmutableSet.of("Mem", "CPU"), actionApiDTO.getRisk().getReasonCommodities());
    }

    /**
     * If move action doesn't have the source entity/id, it's START except Storage;
     *
     * @throws Exception
     */
    @Test
    public void testMapDiskArrayMoveWithoutSourceId() throws Exception {
        ActionInfo moveInfo = getMoveActionInfo(ApiEntityType.DISKARRAY.apiStr(), false);
        Explanation compliance = Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setCompliance(Compliance.newBuilder()
                                        .addMissingCommodities(MEM)
                                        .addMissingCommodities(CPU).build())
                                .build())
                        .build())
                .build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        ActionApiDTO actionApiDTO =
                mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, compliance), REAL_TIME_TOPOLOGY_CONTEXT_ID);

        verify(repositoryApi).entitiesRequest(Sets.newHashSet(3L, 2L));
        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("3", actionApiDTO.getTarget().getUuid());

        assertEquals(1, actionApiDTO.getCompoundActions().size());
        ActionApiDTO first = actionApiDTO.getCompoundActions().get(0);
        assertEquals(DESTINATION, first.getNewEntity().getDisplayName());
        assertEquals("2", first.getNewValue());

        assertEquals(ActionType.START, actionApiDTO.getActionType());
        assertEquals("default explanation", actionApiDTO.getRisk().getDescription());
        assertEquals(ImmutableSet.of("Mem", "CPU"), actionApiDTO.getRisk().getReasonCommodities());
    }

    /**
     * Test map reconfigure action to ActionApiDTO with settings policy information populated.
     */
    @Test
    public void testMapReconfigureWithSettingsPolicy() {
        Map<Long, ApiPartialEntity> entitiesMap = new HashMap<>();
        ApiPartialEntity entity = topologyEntityDTO("Test Entity", 3L, EntityType.VIRTUAL_MACHINE_VALUE);
        entitiesMap.put(1L, entity);
        Set<Long> settingsPolicyIds = new HashSet<>();
        long firstPolicyOid = 12345L;
        long secondPolicyOid = 67890L;
        final String className = "SettingPolicy";
        settingsPolicyIds.add(firstPolicyOid);
        settingsPolicyIds.add(secondPolicyOid);
        Map<Long, PolicyApiDTO> policyApiDto = new HashMap<>();
        Map< Long, BaseApiDTO> relatedSettingsPolicies = new HashMap<>();
        BaseApiDTO settingsPolicyDto = createPolicyBaseApiDTO(firstPolicyOid, "Test1", className);
        BaseApiDTO settingsPolicyDto1 = createPolicyBaseApiDTO(secondPolicyOid, "Test2", className);

        relatedSettingsPolicies.put(firstPolicyOid, settingsPolicyDto);
        relatedSettingsPolicies.put(secondPolicyOid, settingsPolicyDto1);
        ActionSpecMappingContext context = new ActionSpecMappingContext(entitiesMap,
                Collections.emptyMap(), Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
                Collections.emptyMap(), serviceEntityMapper, false, policyApiDto, relatedSettingsPolicies, Collections.emptyMap());

        ActionInfo info =
                ActionInfo.newBuilder().setReconfigure(
                                Reconfigure.newBuilder()
                                        .setTarget(ApiUtilsTest.createActionEntityForCloud(3))
                                        .setSource(ApiUtilsTest.createActionEntityForCloud(1))
                                        .setIsProvider(false)
                                        .build())
                        .build();
        Explanation reconfigure =
                Explanation.newBuilder()
                        .setReconfigure(ReconfigureExplanation.newBuilder()
                                .addAllReasonSettings(settingsPolicyIds).build())
                        .build();

        Action reconf = Action.newBuilder().setInfo(info).setId(2222L).setExplanation(reconfigure)
                .setDeprecatedImportance(1).build();
        ActionApiDTO output = new ActionApiDTO();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        mapper.populateSettingsPolicyForActionApiDto(reconf, output, context);

        assertEquals(2, output.getRelatedSettingsPolicies().stream().count());
    }

    /**
     * Test map reconfigure action to ActionApiDTO with policy information populated.
     */
    @Test
    public void testMapReconfigureWithPolicy() {
        Map<Long, ApiPartialEntity> entitiesMap = new HashMap<>();
        ApiPartialEntity entity = topologyEntityDTO("Test Entity", 3L, EntityType.VIRTUAL_MACHINE_VALUE);
        entitiesMap.put(1L, entity);
        long policyOid = 12345L;
        String policyName = "Test";
        Map<Long, PolicyApiDTO> policyApiDto = new HashMap<>();
        policyApiDto.put(policyOid, createPolicyApiDTO(policyOid, policyName));

        ActionSpecMappingContext context = new ActionSpecMappingContext(entitiesMap,
            Collections.emptyMap(), Collections.emptyMap(),
            Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
            Collections.emptyMap(), serviceEntityMapper, false, policyApiDto, Collections.emptyMap(), Collections.emptyMap());

        final ReasonCommodity placement =
                        createReasonCommodity(CommodityDTO.CommodityType.SEGMENTATION_VALUE, String.valueOf(policyOid));
        ActionInfo info =
                    ActionInfo.newBuilder().setReconfigure(
                            Reconfigure.newBuilder()
                            .setTarget(ApiUtilsTest.createActionEntity(3))
                            .setSource(ApiUtilsTest.createActionEntity(1))
                            .setIsProvider(false)
                            .build())
                    .build();
        Explanation reconfigurePlacementComm =
                    Explanation.newBuilder()
                            .setReconfigure(ReconfigureExplanation.newBuilder()
                                    .addReconfigureCommodity(placement).build())
                            .build();

        Action reconfWithPlacement = Action.newBuilder().setInfo(info).setId(2222L).setExplanation(reconfigurePlacementComm)
                .setDeprecatedImportance(1).build();
        ActionApiDTO output = new ActionApiDTO();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        mapper.populatePolicyForActionApiDto(reconfWithPlacement, output, context);
        assertNotNull(output.getPolicy());
        assertEquals(policyName, output.getPolicy().getDisplayName());
        assertEquals(String.valueOf(policyOid), output.getPolicy().getUuid());
    }

    @Test
    public void testMapReconfigure() throws Exception {
        final ReasonCommodity cpuAllocation =
                        createReasonCommodity(CommodityDTO.CommodityType.CPU_ALLOCATION_VALUE, null);
        final ReasonCommodity network =
                        createReasonCommodity(CommodityDTO.CommodityType.NETWORK_VALUE, "TestNetworkName1");

        ActionInfo moveInfo =
                    ActionInfo.newBuilder().setReconfigure(
                            Reconfigure.newBuilder()
                            .setTarget(ApiUtilsTest.createActionEntity(3))
                            .setSource(ApiUtilsTest.createActionEntity(1))
                            .setIsProvider(false)
                            .build())
                    .build();
        Explanation reconfigure =
                    Explanation.newBuilder()
                            .setReconfigure(ReconfigureExplanation.newBuilder()
                                    .addReconfigureCommodity(cpuAllocation)
                                    .addReconfigureCommodity(network).build())
                            .build();

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(topologyEntityDTOList(Lists.newArrayList(
            new TestEntity(TARGET, 3L, EntityType.VIRTUAL_MACHINE_VALUE),
                new TestEntity(SOURCE, 1L, EntityType.PHYSICAL_MACHINE_VALUE),
                new TestEntity(DC1_NAME, DATACENTER1_ID, EntityType.DATACENTER_VALUE),
                new TestEntity(DC2_NAME, DATACENTER2_ID, EntityType.DATACENTER_VALUE))));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(3L, 1L)))
            .thenReturn(req);
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);

        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, reconfigure), CONTEXT_ID);

        // Verify that we set the context ID on the request.
        verify(req).contextId(CONTEXT_ID);

        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("3", actionApiDTO.getTarget().getUuid());
        assertEquals("VirtualMachine", actionApiDTO.getTarget().getClassName());

        assertEquals(SOURCE, actionApiDTO.getCurrentEntity().getDisplayName());
        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("1", actionApiDTO.getCurrentValue());

        assertEquals(ActionType.RECONFIGURE, actionApiDTO.getActionType());

        assertEquals(DC1_NAME, actionApiDTO.getCurrentLocation().getDisplayName());
        assertEquals(DC2_NAME, actionApiDTO.getNewLocation().getDisplayName());

        assertEquals(ImmutableSet.of("NetworkCommodity", "CPUAllocation"),
            actionApiDTO.getRisk().getReasonCommodities());
    }

    @Test
    public void testMapSourcelessReconfigure() throws Exception {
        final ReasonCommodity cpuAllocation =
                        createReasonCommodity(CommodityDTO.CommodityType.CPU_ALLOCATION_VALUE, null);

        ActionInfo moveInfo =
                    ActionInfo.newBuilder().setReconfigure(
                            Reconfigure.newBuilder()
                            .setTarget(ApiUtilsTest.createActionEntity(3))
                            .setIsProvider(false)
                            .build())
                    .build();
        Explanation reconfigure =
                    Explanation.newBuilder()
                            .setReconfigure(ReconfigureExplanation.newBuilder()
                                    .addReconfigureCommodity(cpuAllocation).build())
                            .build();

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(topologyEntityDTOList(Lists.newArrayList(
            new TestEntity(TARGET, 3L, EntityType.VIRTUAL_MACHINE_VALUE),
                new TestEntity(DC1_NAME, DATACENTER1_ID, EntityType.DATACENTER_VALUE),
                new TestEntity(DC2_NAME, DATACENTER2_ID, EntityType.DATACENTER_VALUE))));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(3L)))
            .thenReturn(req);

        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, reconfigure), CONTEXT_ID);

        // Verify that we set the context ID on the request.
        verify(req).contextId(CONTEXT_ID);

        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("3", actionApiDTO.getTarget().getUuid());
        assertEquals("VirtualMachine", actionApiDTO.getTarget().getClassName());

        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());

        assertEquals( ActionType.RECONFIGURE, actionApiDTO.getActionType());
        assertNull(actionApiDTO.getCurrentLocation());
        assertEquals(DC2_NAME, actionApiDTO.getNewLocation().getDisplayName());
    }

    @Test
    public void testMapProvisionPlan() throws Exception {
        ActionInfo provisionInfo =
            ActionInfo.newBuilder()
                .setProvision(Provision.newBuilder()
                    .setEntityToClone(ApiUtilsTest.createActionEntity(3))
                    .setProvisionedSeller(2).build()).build();
        Explanation provision = Explanation.newBuilder().setProvision(ProvisionExplanation
            .newBuilder().setProvisionBySupplyExplanation(ProvisionBySupplyExplanation
                .newBuilder().setMostExpensiveCommodityInfo(
                    ReasonCommodity.newBuilder().setCommodityType(
                        CommodityType.newBuilder().setType(21).build())
                        .build())).build()).build();

        final MultiEntityRequest allReq = ApiTestUtils.mockMultiEntityReq(topologyEntityDTOList(Lists.newArrayList(
            new TestEntity("EntityToClone", 3L, EntityType.VIRTUAL_MACHINE_VALUE),
            new TestEntity("EntityToClone", 2L, EntityType.VIRTUAL_MACHINE_VALUE),
            new TestEntity(DC1_NAME, DATACENTER1_ID, EntityType.DATACENTER_VALUE),
            new TestEntity(DC2_NAME, DATACENTER2_ID, EntityType.DATACENTER_VALUE))));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(3L, 2L)))
            .thenReturn(allReq);

        final long planId = 1 + REAL_TIME_TOPOLOGY_CONTEXT_ID;
        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
            buildActionSpec(provisionInfo, provision), planId);

        // Verify that we set the context ID on the request.
        verify(allReq).contextId(planId);

        assertEquals("EntityToClone", actionApiDTO.getCurrentEntity().getDisplayName());
        assertEquals("3", actionApiDTO.getCurrentValue());

        assertEquals("EntityToClone", actionApiDTO.getTarget().getDisplayName());
        assertEquals("VirtualMachine", actionApiDTO.getTarget().getClassName());
        assertEquals("3", actionApiDTO.getTarget().getUuid());

        assertEquals("EntityToClone", actionApiDTO.getNewEntity().getDisplayName());
        assertEquals("VirtualMachine", actionApiDTO.getNewEntity().getClassName());
        assertEquals("2", actionApiDTO.getNewEntity().getUuid());

        assertEquals(ActionType.PROVISION, actionApiDTO.getActionType());
        assertEquals(DC2_NAME, actionApiDTO.getCurrentLocation().getDisplayName());
        assertEquals(DC2_NAME, actionApiDTO.getNewLocation().getDisplayName());
        assertEquals(ImmutableSet.of("Mem"), actionApiDTO.getRisk().getReasonCommodities());
    }

    @Test
    public void testMapProvisionRealtime() throws Exception {
        ActionInfo provisionInfo =
                ActionInfo.newBuilder()
                    .setProvision(Provision.newBuilder()
                        .setEntityToClone(ApiUtilsTest.createActionEntity(3))
                        .setProvisionedSeller(-1).build()).build();
        Explanation provision = Explanation.newBuilder().setProvision(ProvisionExplanation.newBuilder()
            .setProvisionByDemandExplanation(
                ProvisionByDemandExplanation.newBuilder()
                    .setBuyerId(11)
                    .addCommodityNewCapacityEntry(
                        CommodityNewCapacityEntry.newBuilder().setCommodityBaseType(21).setNewCapacity(10))
                    .addCommodityNewCapacityEntry(
                        CommodityNewCapacityEntry.newBuilder().setCommodityBaseType(40).setNewCapacity(10)))).build();

        final MultiEntityRequest srcReq = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO("EntityToClone", 3L, EntityType.VIRTUAL_MACHINE_VALUE)));
        final MultiEntityRequest projReq = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO("EntityToClone", -1, EntityType.VIRTUAL_MACHINE_VALUE)));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(3L)))
            .thenReturn(srcReq);
        when(repositoryApi.entitiesRequest(Sets.newHashSet(-1L)))
            .thenReturn(projReq);

        topologyEntityDTOList(Lists.newArrayList(
                new TestEntity("EntityToClone", 3L, EntityType.VIRTUAL_MACHINE_VALUE),
                new TestEntity("EntityToClone", -1, EntityType.VIRTUAL_MACHINE_VALUE),
                new TestEntity(DC1_NAME, DATACENTER1_ID, EntityType.DATACENTER_VALUE),
                new TestEntity(DC2_NAME, DATACENTER2_ID, EntityType.DATACENTER_VALUE)));


        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
                buildActionSpec(provisionInfo, provision), REAL_TIME_TOPOLOGY_CONTEXT_ID);

        // Verify that we set the context ID on the request.
        verify(srcReq).contextId(REAL_TIME_TOPOLOGY_CONTEXT_ID);

        assertEquals("EntityToClone", actionApiDTO.getCurrentEntity().getDisplayName());
        assertEquals("3", actionApiDTO.getCurrentValue());

        assertEquals("EntityToClone", actionApiDTO.getTarget().getDisplayName());
        assertEquals("VirtualMachine", actionApiDTO.getTarget().getClassName());
        assertEquals("3", actionApiDTO.getTarget().getUuid());

        // Should be empty in realtime - we don't provide a reference to the provisioned entity.
        assertNull(actionApiDTO.getNewEntity().getDisplayName());
        assertNull(actionApiDTO.getNewEntity().getClassName());
        assertNull(actionApiDTO.getNewEntity().getUuid());

        assertEquals(ActionType.PROVISION, actionApiDTO.getActionType());
        assertEquals(DC2_NAME, actionApiDTO.getCurrentLocation().getDisplayName());
        assertEquals(DC2_NAME, actionApiDTO.getNewLocation().getDisplayName());

        assertEquals(ImmutableSet.of("Mem", "CPU"), actionApiDTO.getRisk().getReasonCommodities());
    }

    /**
     * Test the mapping of action data for vSAN causing host provision action.
     * @throws Exception that encompasses possible exceptions
     */
    @Test
    public void testMapProvisionCausedByVSAN() throws Exception  {
        final long clonedEntityId = 3;
        final String clonedEntityName = "EntityToClone";

        final ActionInfo provisionActionInfo = ActionInfo.newBuilder().setProvision(
                        Provision.newBuilder()
                            .setEntityToClone(ApiUtilsTest.createActionEntity(clonedEntityId))
                            .setProvisionedSeller(-1).build())
                        .build();
        final Explanation provisionExplanation = Explanation.newBuilder().setProvision(
                        ProvisionExplanation.newBuilder().setProvisionBySupplyExplanation(
                            ProvisionBySupplyExplanation.newBuilder().setMostExpensiveCommodityInfo(
                                ReasonCommodity.newBuilder().setCommodityType(
                                    CommodityType.newBuilder().setType(
                                        CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE).build())
                                .build()))
                        .build()).build();

        final MultiEntityRequest srcReq = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
                        topologyEntityDTO(clonedEntityName, clonedEntityId, EntityType.PHYSICAL_MACHINE_VALUE)));
        final MultiEntityRequest projReq = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
                        topologyEntityDTO(clonedEntityName, -1, EntityType.PHYSICAL_MACHINE_VALUE)));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(clonedEntityId))).thenReturn(srcReq);
        when(repositoryApi.entitiesRequest(Sets.newHashSet(-1L))).thenReturn(projReq);

        BaseApiDTO vsanEntity = new BaseApiDTO();
        vsanEntity.setUuid("vsanID");
        vsanEntity.setClassName(StringConstants.STORAGE);
        vsanEntity.setDisplayName("vsan_entity_name");
        TestEntity clonedEntity = new TestEntity(clonedEntityName, clonedEntityId, EntityType.PHYSICAL_MACHINE_VALUE);
        clonedEntity.consumers = new ArrayList<BaseApiDTO>(1);
        clonedEntity.consumers.add(vsanEntity);
        topologyEntityDTOList(Lists.newArrayList(clonedEntity,
                        new TestEntity(clonedEntityName, -1, EntityType.PHYSICAL_MACHINE_VALUE),
                        new TestEntity(DC1_NAME, DATACENTER1_ID, EntityType.DATACENTER_VALUE),
                        new TestEntity(DC2_NAME, DATACENTER2_ID, EntityType.DATACENTER_VALUE)));

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
                        buildActionSpec(provisionActionInfo, provisionExplanation),
                        REAL_TIME_TOPOLOGY_CONTEXT_ID);

        assertEquals(vsanEntity.getUuid(), actionApiDTO.getNewEntity().getUuid());
        assertEquals(vsanEntity.getClassName(), actionApiDTO.getNewEntity().getClassName());
        assertEquals(vsanEntity.getDisplayName(), actionApiDTO.getNewEntity().getDisplayName());

        assertEquals(ImmutableSet.of("StorageAmount"), actionApiDTO.getRisk().getReasonCommodities());
    }

    /**
     * Test the data for {@link CloudResizeActionDetailsApiDTO}
     * is populated from {@link CloudSavingsDetails}.
     */
    @Test
    public void testMapFromCloudSavingsDetailsDtoToCloudResizeActionDetailsApiDTO() {
        final long targetId = 1;
        final CloudSavingsDetails cloudSavingsDetails =
                CloudSavingsDetails.newBuilder()
                        .setSourceTierCostDetails(TierCostDetails.newBuilder()
                                .setCloudCommitmentCoverage(CloudCommitmentCoverage.newBuilder()
                                        .setCapacity(
                                                CloudCommitmentAmount.newBuilder().setCoupons(4))
                                        .setUsed(CloudCommitmentAmount.newBuilder().setCoupons(1)))
                                .setOnDemandCost(CurrencyAmount.newBuilder().setAmount(10))
                                .setOnDemandRate(CurrencyAmount.newBuilder().setAmount(0)))
                        .setProjectedTierCostDetails(
                                TierCostDetails.newBuilder()
                                        .setCloudCommitmentCoverage(
                                                CloudCommitmentCoverage.newBuilder()
                                                        .setCapacity(
                                                                CloudCommitmentAmount.newBuilder()
                                                                        .setCoupons(4))
                                                        .setUsed(CloudCommitmentAmount.newBuilder()
                                                                .setCoupons(1)))
                                        .setOnDemandCost(CurrencyAmount.newBuilder().setAmount(20))
                                        .setOnDemandRate(CurrencyAmount.newBuilder().setAmount(0)))
                        .setEntityUptime(EntityUptimeDTO.newBuilder())
                        .build();

        final ActionEntity actionEntity = ActionEntity.newBuilder().setId(targetId).setType(
                EntityType.VIRTUAL_MACHINE_VALUE).setEnvironmentType(
                EnvironmentTypeEnum.EnvironmentType.CLOUD).build();

        Stream.of(ActionInfo.newBuilder()
                .setScale(Scale.newBuilder()
                        .setTarget(actionEntity)
                        .setCloudSavingsDetails(cloudSavingsDetails))
                .build(), ActionInfo.newBuilder().setAllocate(Allocate.newBuilder()
                .setTarget(actionEntity)
                .setWorkloadTier(ApiUtilsTest.createActionEntity(4))
                .setCloudSavingsDetails(cloudSavingsDetails)).build()).forEach(actionInfo -> {

            Mockito.reset(cloudSavingsDetailsDtoConverter);

            final ActionSpec actionSpec = ActionSpec.newBuilder().setRecommendation(
                    Action.newBuilder()
                            .setId(ACTION_STABLE_IMPACT_ID)
                            .setInfo(actionInfo)
                            .setDeprecatedImportance(0)
                            .setExplanation(Explanation.newBuilder())).build();

            final ActionOrchestratorAction actionOrchestratorAction =
                    ActionOrchestratorAction.newBuilder()
                            .setActionId(ACTION_LEGACY_INSTANCE_ID)
                            .setActionSpec(actionSpec)
                            .build();

            // act
            final CloudResizeActionDetailsApiDTO cloudResizeActionDetailsApiDTO =
                    (CloudResizeActionDetailsApiDTO)mapper.createActionDetailsApiDTO(
                            actionOrchestratorAction, REAL_TIME_TOPOLOGY_CONTEXT_ID);

            // check
            assertNotNull(cloudResizeActionDetailsApiDTO);
            // on-demand cost
            assertEquals(10, cloudResizeActionDetailsApiDTO.getOnDemandCostBefore(), 0);
            assertEquals(20, cloudResizeActionDetailsApiDTO.getOnDemandCostAfter(), 0);
            // not implemented yet - set to $0 by default
            assertEquals(0, cloudResizeActionDetailsApiDTO.getOnDemandRateBefore(), 0);
            assertEquals(0, cloudResizeActionDetailsApiDTO.getOnDemandRateAfter(), 0);
            // ri coverage
            assertEquals(1, cloudResizeActionDetailsApiDTO.getRiCoverageBefore().getValue(), 0);
            assertEquals(4,
                    cloudResizeActionDetailsApiDTO.getRiCoverageBefore().getCapacity().getAvg(), 0);
            assertEquals(1, cloudResizeActionDetailsApiDTO.getRiCoverageAfter().getValue(), 0);
            assertEquals(4,
                    cloudResizeActionDetailsApiDTO.getRiCoverageAfter().getCapacity().getAvg(), 0);
            assertNotNull(cloudResizeActionDetailsApiDTO.getEntityUptime());
            Mockito.verify(cloudSavingsDetailsDtoConverter, Mockito.times(1)).convert(
                    Mockito.any());
        });
    }

    /**
     * Test the data for {@link CloudResizeActionDetailsApiDTO}
     * is populated from {@link CloudSavingsDetails}.
     * Expect getting RI Buy data from grpc.
     */
    @Test
    public void testMCPMoveActionRIBuy() {
        final CloudSavingsDetails cloudSavingsDetails =
                CloudSavingsDetails.newBuilder()
                        .setSourceTierCostDetails(TierCostDetails.newBuilder()
                                .setCloudCommitmentCoverage(CloudCommitmentCoverage.newBuilder()
                                        .setCapacity(
                                                CloudCommitmentAmount.newBuilder().setCoupons(4))
                                        .setUsed(CloudCommitmentAmount.newBuilder().setCoupons(1)))
                                .setOnDemandCost(CurrencyAmount.newBuilder().setAmount(10))
                                .setOnDemandRate(CurrencyAmount.newBuilder().setAmount(0)))
                        .setProjectedTierCostDetails(
                                TierCostDetails.newBuilder()
                                        .setCloudCommitmentCoverage(
                                                CloudCommitmentCoverage.newBuilder()
                                                        .setCapacity(
                                                                CloudCommitmentAmount.newBuilder()
                                                                        .setCoupons(4))
                                                        .setUsed(CloudCommitmentAmount.newBuilder()
                                                                .setCoupons(1)))
                                        .setOnDemandCost(CurrencyAmount.newBuilder().setAmount(20))
                                        .setOnDemandRate(CurrencyAmount.newBuilder().setAmount(0)))
                        .setEntityUptime(EntityUptimeDTO.newBuilder().setUptimePercentage(1.0))
                        .build();

        final ActionEntity actionEntity = ActionEntity.newBuilder()
                                                    .setId(TARGET_ID)
                                                    .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                                    .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                                                    .build();

        ActionInfo actionInfo = ActionInfo.newBuilder()
                                .setMove(Move.newBuilder()
                                            .setTarget(actionEntity)
                                            .addChanges(ChangeProvider.newBuilder()
                                                    .setSource(ApiUtilsTest.createActionEntity(2))
                                                    .setDestination(ApiUtilsTest.createActionEntity(3))
                                                    .build())
                                            .setCloudSavingsDetails(cloudSavingsDetails)
                                            .build())
                                .build();
        Mockito.reset(cloudSavingsDetailsDtoConverter);
        final ActionSpec actionSpec = ActionSpec.newBuilder().setRecommendation(
                Action.newBuilder()
                        .setId(ACTION_STABLE_IMPACT_ID)
                        .setInfo(actionInfo)
                        .setDeprecatedImportance(0)
                        .setExplanation(Explanation.newBuilder())).build();

        final ActionOrchestratorAction actionOrchestratorAction =
                ActionOrchestratorAction.newBuilder()
                        .setActionId(ACTION_LEGACY_INSTANCE_ID)
                        .setActionSpec(actionSpec)
                        .build();
        // act
        final CloudResizeActionDetailsApiDTO cloudResizeActionDetailsApiDTO =
                (CloudResizeActionDetailsApiDTO) mapper.createActionDetailsApiDTO(actionOrchestratorAction, REAL_TIME_TOPOLOGY_CONTEXT_ID);
        // check
        assertNotNull(cloudResizeActionDetailsApiDTO);
        // ri coverage
        assertEquals(1f, cloudResizeActionDetailsApiDTO.getRiCoverageBefore().getCapacity().getAvg(), 0);
        assertEquals(8f, cloudResizeActionDetailsApiDTO.getRiCoverageAfter().getCapacity().getAvg(), 0);
        assertEquals(1.0, cloudResizeActionDetailsApiDTO.getEntityUptime().getUptimePercentage(), 0);
    }

    /**
     * Test the data for {@link OnPremResizeActionDetailsApiDTO}
     */
    @Test
    public void testOnPremResizeActionDetailsApiDTO() {

        final ActionEntity actionEntity = ActionEntity.newBuilder()
                .setId(TARGET_ID)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                .build();

        ActionInfo actionInfo = ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                        .setTarget(actionEntity)
                        .setNewCapacity(4).setOldCapacity(2).setOldCpsr(1).setNewCpsr(4)
                        .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
                )
                .build();
        final ActionSpec actionSpec = ActionSpec.newBuilder().setRecommendation(
                Action.newBuilder()
                        .setId(ACTION_STABLE_IMPACT_ID)
                        .setInfo(actionInfo)
                        .setDeprecatedImportance(0)
                        .setExplanation(Explanation.newBuilder())).build();

        final ActionOrchestratorAction actionOrchestratorAction =
                ActionOrchestratorAction.newBuilder()
                        .setActionId(ACTION_LEGACY_INSTANCE_ID)
                        .setActionSpec(actionSpec)
                        .build();
        final OnPremResizeActionDetailsApiDTO onPremResizeActionDetailsApiDTO = (OnPremResizeActionDetailsApiDTO)mapper.createActionDetailsApiDTO(actionOrchestratorAction, REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertNotNull(onPremResizeActionDetailsApiDTO);
        assertEquals(2, onPremResizeActionDetailsApiDTO.getVcpuBefore());
        assertEquals(4, onPremResizeActionDetailsApiDTO.getVcpuAfter());
        assertEquals(1, onPremResizeActionDetailsApiDTO.getCoresPerSocketBefore());
        assertEquals(4, onPremResizeActionDetailsApiDTO.getCoresPerSocketAfter());
        assertEquals(2, onPremResizeActionDetailsApiDTO.getSocketsBefore());
        assertEquals(1, onPremResizeActionDetailsApiDTO.getSocketsAfter());
    }

    /**
     * Test the data for {@link OnPremResizeActionDetailsApiDTO}
     */
    @Test
    public void testNoOnPremResizeActionDetailsApiDTOForVMemResize() {

        final ActionEntity actionEntity = ActionEntity.newBuilder()
                .setId(TARGET_ID)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                .build();

        ActionInfo actionInfo = ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                        .setTarget(actionEntity)
                        .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VMEM_VALUE))
                )
                .build();
        final ActionSpec actionSpec = ActionSpec.newBuilder().setRecommendation(
                Action.newBuilder()
                        .setId(ACTION_STABLE_IMPACT_ID)
                        .setInfo(actionInfo)
                        .setDeprecatedImportance(0)
                        .setExplanation(Explanation.newBuilder())).build();

        final ActionOrchestratorAction actionOrchestratorAction =
                ActionOrchestratorAction.newBuilder()
                        .setActionId(ACTION_LEGACY_INSTANCE_ID)
                        .setActionSpec(actionSpec)
                        .build();
        final OnPremResizeActionDetailsApiDTO onPremResizeActionDetailsApiDTO = (OnPremResizeActionDetailsApiDTO)mapper.createActionDetailsApiDTO(actionOrchestratorAction, REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertNull(onPremResizeActionDetailsApiDTO);
    }


    /**
     * Test that the on-demand cost and on-demand rate values can be properly aggregated and
     * populated in the newly constructed action details.
     */
    @Test
    public void testCloudProvisionSuspendActionDetails() {
        // Set up
        final long provisionedVMId = 1;
        final long suspendedVMId = 2;
        // The OnDemand cost has two records, one simulates the compute cost, the other simulates
        // the license cost. The RPC call can get both values in one call.
        final Cost.CloudCostStatRecord provisionedVMRecord = Cost.CloudCostStatRecord.newBuilder()
                .addStatRecords(Cost.CloudCostStatRecord.StatRecord.newBuilder()
                        .setValues(Cost.StatValue.newBuilder()
                                .setAvg(10f)
                                .setTotal(10f))
                        .setAssociatedEntityId(provisionedVMId))
                .addStatRecords(Cost.CloudCostStatRecord.StatRecord.newBuilder()
                        .setValues(Cost.StatValue.newBuilder()
                                .setAvg(20f)
                                .setTotal(20f))
                        .setAssociatedEntityId(provisionedVMId))
                .build();
        final Cost.CloudCostStatRecord suspendedVMRecord = Cost.CloudCostStatRecord.newBuilder()
                .addStatRecords(Cost.CloudCostStatRecord.StatRecord.newBuilder()
                        .setValues(Cost.StatValue.newBuilder()
                                .setAvg(30f)
                                .setTotal(30f))
                        .setAssociatedEntityId(suspendedVMId))
                .addStatRecords(Cost.CloudCostStatRecord.StatRecord.newBuilder()
                        .setValues(Cost.StatValue.newBuilder()
                                .setAvg(40f)
                                .setTotal(40f))
                        .setAssociatedEntityId(suspendedVMId))
                .build();
        final Cost.GetCloudCostStatsResponse costResult = Cost.GetCloudCostStatsResponse
                .newBuilder()
                .addCloudStatRecord(provisionedVMRecord)
                .addCloudStatRecord(suspendedVMRecord)
                .build();
        // For OnDemand rate, we need to issue two separate calls for compute rate, and license rate
        // respectively. In this test, I am assuming both calls will return the same rateResult.
        final Cost.GetTierPriceForEntitiesResponse rateResult = Cost.GetTierPriceForEntitiesResponse
                .newBuilder()
                .putBeforeTierPriceByEntityOid(provisionedVMId,
                        CurrencyAmount.newBuilder().setAmount(17d).build())
                .putBeforeTierPriceByEntityOid(suspendedVMId,
                        CurrencyAmount.newBuilder().setAmount(27d).build())
                .build();
        ArgumentCaptor<Cost.GetCloudCostStatsRequest> costParamCaptor =
                ArgumentCaptor.forClass(Cost.GetCloudCostStatsRequest.class);
        when(costServiceMole.getCloudCostStats(costParamCaptor.capture()))
                .thenReturn(Collections.singletonList(costResult));
        ArgumentCaptor<Cost.GetTierPriceForEntitiesRequest> rateParamCaptor =
                ArgumentCaptor.forClass(Cost.GetTierPriceForEntitiesRequest.class);
        when(costServiceMole.getTierPriceForEntities(rateParamCaptor.capture()))
                .thenReturn(rateResult);
        // Test
        Map<Long, ActionDetailsApiDTO> provisionActionDetails = new HashMap<>();
        Map<Long, ActionDetailsApiDTO> suspendActionDetails = new HashMap<>();
                mapper.createCloudProvisionSuspendActionDetailsDTOs(
                        Collections.singleton(provisionedVMId),
                        Collections.singleton(suspendedVMId),
                        REAL_TIME_TOPOLOGY_CONTEXT_ID,
                        provisionActionDetails,
                        suspendActionDetails);
        assertEquals(1, provisionActionDetails.size());
        assertEquals(1, suspendActionDetails.size());
        final ActionDetailsApiDTO provision = provisionActionDetails.get(provisionedVMId);
        final ActionDetailsApiDTO suspend = suspendActionDetails.get(suspendedVMId);
        assertNotNull(provision);
        assertNotNull(suspend);
        // Assert that we get the correct result:
        // OnDemandRate (two calls returning the same result):
        //   - Provision  17 + 17 = 34
        //   - Suspension 27 + 27 = 54
        assertEquals(34, ((CloudProvisionActionDetailsApiDTO)provision).getOnDemandRate(), 0);
        assertEquals(54, ((CloudSuspendActionDetailsApiDTO)suspend).getOnDemandRate(), 0);
        // OnDemandCost:
        //   - Provision  10 + 20 = 30
        //   - Suspension 30 + 40 = 70
        assertEquals(30, ((CloudProvisionActionDetailsApiDTO)provision).getOnDemandCost(), 0);
        assertEquals(70, ((CloudSuspendActionDetailsApiDTO)suspend).getOnDemandCost(), 0);
    }

    @Test
    public void testMapResize() throws Exception {
        final long targetId = 1;
        final ActionInfo resizeInfo = ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                    .setTarget(ApiUtilsTest.createActionEntity(targetId))
                    .setOldCapacity(9)
                    .setNewCapacity(10)
                    .setCommodityType(CPU.getCommodityType()))
            .build();

        Explanation resize = Explanation.newBuilder()
            .setResize(ResizeExplanation.newBuilder()
                .setDeprecatedStartUtilization(0.2f)
                .setDeprecatedEndUtilization(0.4f).build())
            .build();

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(topologyEntityDTOList(Lists.newArrayList(
                new TestEntity(ENTITY_TO_RESIZE_NAME, targetId, EntityType.VIRTUAL_MACHINE_VALUE),
                new TestEntity(DC1_NAME, DATACENTER1_ID, EntityType.DATACENTER_VALUE),
                new TestEntity(DC2_NAME, DATACENTER2_ID, EntityType.DATACENTER_VALUE))));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(targetId)))
            .thenReturn(req);

        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(buildActionSpec(resizeInfo, resize), CONTEXT_ID);

        // Verify that we set the context ID on the request.
        verify(req).contextId(CONTEXT_ID);

        assertEquals(ENTITY_TO_RESIZE_NAME, actionApiDTO.getTarget().getDisplayName());
        assertEquals(targetId, Long.parseLong(actionApiDTO.getTarget().getUuid()));
        assertEquals(ActionType.RESIZE, actionApiDTO.getActionType());
        assertEquals(1, actionApiDTO.getRisk().getReasonCommodities().size());
        assertEquals(CommodityDTO.CommodityType.CPU.name(),
                actionApiDTO.getRisk().getReasonCommodities().iterator().next());
        assertEquals(DC1_NAME, actionApiDTO.getCurrentLocation().getDisplayName());
        assertEquals(DC1_NAME, actionApiDTO.getNewLocation().getDisplayName());

        assertEquals(ImmutableSet.of("CPU"), actionApiDTO.getRisk().getReasonCommodities());
    }

    /**
     * Test the mapping of atomic resizes.
     * @throws Exception exception can be thrown from mapActionSpecToActionApiDTO
     */
    @Test
    public void testMapAtomicResize() throws Exception {
        final long targetId = 1;
        final ActionInfo resizeInfo = ActionInfo.newBuilder()
            .setAtomicResize(AtomicResize.newBuilder()
                .setExecutionTarget(ApiUtilsTest.createActionEntity(targetId))
                .addResizes(ResizeInfo.newBuilder()
                    .setCommodityType(CPU.getCommodityType())
                    .setTarget(ApiUtilsTest.createActionEntity(targetId))
                    .setOldCapacity(9)
                    .setNewCapacity(10)))
            .build();

        Explanation resizeExplanation = Explanation.newBuilder()
            .setResize(ResizeExplanation.newBuilder()
                .setDeprecatedStartUtilization(0.2f)
                .setDeprecatedEndUtilization(0.4f).build())
            .build();

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(topologyEntityDTOList(Lists.newArrayList(
            new TestEntity(ENTITY_TO_RESIZE_NAME, targetId, EntityType.VIRTUAL_MACHINE_VALUE))));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(targetId)))
            .thenReturn(req);

        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(buildActionSpec(resizeInfo, resizeExplanation), CONTEXT_ID);

        assertEquals(ENTITY_TO_RESIZE_NAME, actionApiDTO.getTarget().getDisplayName());
        assertEquals(1, actionApiDTO.getCompoundActions().size());
        ActionApiDTO subAction = actionApiDTO.getCompoundActions().get(0);
        assertEquals(9.0f, Float.parseFloat(subAction.getCurrentValue()), 0.01);
        assertEquals(10.0f, Float.parseFloat(subAction.getNewValue()), 0.01);
        assertEquals("CPU Congestion in EntityToResize", subAction.getDetails());
        assertEquals(targetId, Long.parseLong(subAction.getTarget().getUuid()));
        assertEquals(ActionType.RESIZE, subAction.getActionType());
    }

    @Test
    public void testResizeVMemDetail() throws Exception {
        final long targetId = 1;
        final ActionInfo resizeInfo = ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                        .setTarget(ApiUtilsTest.createActionEntity(targetId))
                        .setOldCapacity(1024 * 1024 * 2)
                        .setNewCapacity(1024 * 1024 * 1)
                        .setCommodityType(VMEM.getCommodityType()))
                .build();
        Explanation resize = Explanation.newBuilder()
                .setResize(ResizeExplanation.newBuilder()
                        .setDeprecatedStartUtilization(0.2f)
                        .setDeprecatedEndUtilization(0.4f).build())
                .build();
        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO(ENTITY_TO_RESIZE_NAME, targetId, EntityType.VIRTUAL_MACHINE_VALUE)));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(targetId)))
            .thenReturn(req);

        final ActionApiDTO actionApiDTO =
                mapper.mapActionSpecToActionApiDTO(buildActionSpec(resizeInfo, resize), CONTEXT_ID);

        // Verify that we set the context ID on the request.
        verify(req).contextId(CONTEXT_ID);

        assertEquals(ActionType.RESIZE, actionApiDTO.getActionType());
        assertEquals(1, actionApiDTO.getRisk().getReasonCommodities().size());
        assertEquals(UICommodityType.VMEM.apiStr(),
                actionApiDTO.getRisk().getReasonCommodities().iterator().next());
        assertEquals("2097152.0", actionApiDTO.getCurrentValue());
        assertEquals("1048576.0", actionApiDTO.getNewValue());
        assertEquals(CommodityTypeMapping.getUnitForCommodityType(CommodityDTO.CommodityType.VMEM),
            actionApiDTO.getValueUnits());
    }

    @Test
    public void testResizeHeapDetail() throws Exception {
        final long targetId = 1;
        final ActionInfo resizeInfo = ActionInfo.newBuilder()
            .setResize(Resize.newBuilder()
                .setTarget(ApiUtilsTest.createActionEntity(targetId))
                .setOldCapacity(1024 * 1024 * 2)
                .setNewCapacity(1024 * 1024 * 1)
                .setCommodityType(HEAP.getCommodityType()))
            .build();
        Explanation resize = Explanation.newBuilder()
            .setResize(ResizeExplanation.newBuilder()
                .setDeprecatedStartUtilization(0.2f)
                .setDeprecatedEndUtilization(0.4f).build())
            .build();

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO(ENTITY_TO_RESIZE_NAME, targetId, EntityType.VIRTUAL_MACHINE_VALUE)));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(targetId)))
            .thenReturn(req);

        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(buildActionSpec(resizeInfo, resize), CONTEXT_ID);

        // Verify that we set the context ID on the request.
        verify(req).contextId(CONTEXT_ID);

        assertEquals(ActionType.RESIZE, actionApiDTO.getActionType());
        assertEquals(1, actionApiDTO.getRisk().getReasonCommodities().size());
        assertEquals(UICommodityType.HEAP.apiStr(), actionApiDTO.getRisk().getReasonCommodities().iterator().next());
    }

    /**
     * Test that large capacities values in resize actions are formatted in a consistent way.
     *
     * @throws Exception when a problem occurs during mapping
     */
    @Test
    public void testResizeLargeCapacities() throws Exception {
        final long targetId = 1;
        final float oldCapacity = 1024f * 1024 * 1024 * 2;
        final float newCapacity = 1024f * 1024 * 1024 * 1;
        final ActionInfo resizeInfo = ActionInfo.newBuilder()
            .setResize(Resize.newBuilder()
                .setTarget(ApiUtilsTest.createActionEntity(targetId))
                .setOldCapacity(oldCapacity)
                .setNewCapacity(newCapacity)
                .setCommodityType(HEAP.getCommodityType()))
            .build();
        Explanation resize = Explanation.newBuilder()
            .setResize(ResizeExplanation.newBuilder()
                .setDeprecatedStartUtilization(0.2f)
                .setDeprecatedEndUtilization(0.4f).build())
            .build();

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO(ENTITY_TO_RESIZE_NAME, targetId, EntityType.VIRTUAL_MACHINE_VALUE)));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(targetId)))
            .thenReturn(req);

        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(buildActionSpec(resizeInfo, resize), CONTEXT_ID);

        // Verify that we set the context ID on the request.
        verify(req).contextId(CONTEXT_ID);

        assertEquals(ActionType.RESIZE, actionApiDTO.getActionType());
        assertEquals(1, actionApiDTO.getRisk().getReasonCommodities().size());
        assertEquals(UICommodityType.HEAP.apiStr(), actionApiDTO.getRisk().getReasonCommodities().iterator().next());
        // Check that the resize values are formatted in a consistent, API backwards-compatible way.
        assertEquals(String.format("%.1f", oldCapacity), actionApiDTO.getCurrentValue());
        assertEquals(String.format("%.1f", newCapacity), actionApiDTO.getNewValue());
    }

    /**
     * Test that Processing Units commodities values in resize actions are formatted in two decimal places.
     *
     * @throws Exception when a problem occurs during mapping
     */
    @Test
    public void testResizeProcessingUnits() throws Exception {
        final long targetId = 1;
        final float oldCapacity = 1.75f;
        final float newCapacity = 2.25f;
        final ActionInfo resizeInfo = ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                        .setTarget(ApiUtilsTest.createActionEntity(targetId))
                        .setOldCapacity(oldCapacity)
                        .setNewCapacity(newCapacity)
                        .setCommodityType(PROCESSING_UNITS.getCommodityType()))
                .build();
        Explanation resize = Explanation.newBuilder()
                .setResize(ResizeExplanation.newBuilder()
                        .setDeprecatedStartUtilization(0.2f)
                        .setDeprecatedEndUtilization(0.4f).build())
                .build();

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
                topologyEntityDTO(ENTITY_TO_RESIZE_NAME, targetId, EntityType.VIRTUAL_MACHINE_VALUE)));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(targetId)))
                .thenReturn(req);

        final ActionApiDTO actionApiDTO =
                mapper.mapActionSpecToActionApiDTO(buildActionSpec(resizeInfo, resize), CONTEXT_ID);

        // Verify that we set the context ID on the request.
        verify(req).contextId(CONTEXT_ID);

        assertEquals(ActionType.RESIZE, actionApiDTO.getActionType());
        assertEquals(1, actionApiDTO.getRisk().getReasonCommodities().size());
        assertEquals(UICommodityType.PROCESSING_UNITS.apiStr(), actionApiDTO.getRisk().getReasonCommodities().iterator().next());
        // Check that the resize values are formatted in a consistent, API backwards-compatible way.
        assertEquals(String.format("%.2f", oldCapacity), actionApiDTO.getCurrentValue());
        assertEquals(String.format("%.2f", newCapacity), actionApiDTO.getNewValue());
    }

    /**
     * Test a limit resize.
     *
     * @throws Exception from mapper.mapActionSpecToActionApiDTO(), but shouldn't
     * happen within this test.
     */
    @Test
    public void testMapResizeVMLimit() throws Exception {
        final long targetId = 1;
        final ActionInfo resizeInfo = ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                        .setTarget(ApiUtilsTest.createActionEntity(targetId,
                                EntityType.VIRTUAL_MACHINE.getNumber()))
                        .setOldCapacity(10)
                        .setNewCapacity(0)
                        .setCommodityAttribute(CommodityAttribute.LIMIT)
                        .setCommodityType(MEM.getCommodityType()))
                .build();

        Explanation resize = Explanation.newBuilder()
                .setResize(ResizeExplanation.newBuilder()
                        .setDeprecatedStartUtilization(0.2f)
                        .setDeprecatedEndUtilization(0.4f).build())
                .build();


        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO(ENTITY_TO_RESIZE_NAME, targetId, EntityType.VIRTUAL_MACHINE_VALUE)));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(targetId)))
            .thenReturn(req);

        final ActionApiDTO actionApiDTO =
                mapper.mapActionSpecToActionApiDTO(buildActionSpec(resizeInfo, resize), CONTEXT_ID);

        // Verify that we set the context ID on the request.
        verify(req).contextId(CONTEXT_ID);
        assertEquals(ENTITY_TO_RESIZE_NAME, actionApiDTO.getTarget().getDisplayName());
        assertEquals(ActionType.RESIZE, actionApiDTO.getActionType());
        assertEquals(1, actionApiDTO.getRisk().getReasonCommodities().size());
        assertEquals(UICommodityType.MEM.apiStr(), actionApiDTO.getRisk().getReasonCommodities().iterator().next());
    }

    /**
     * Action uuid should only be the stable impact id when the feature flag is enabled and from
     * the live market.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testUuidIsStableOnlyWhenEnabledAndLiveMarket() throws Exception {
        final long targetId = 1;
        final ActionInfo resizeInfo = ActionInfo.newBuilder()
            .setResize(Resize.newBuilder()
                .setTarget(ApiUtilsTest.createActionEntity(targetId))
                .setOldCapacity(9)
                .setNewCapacity(10)
                .setCommodityType(CPU.getCommodityType()))
            .build();

        Explanation resize = Explanation.newBuilder()
            .setResize(ResizeExplanation.newBuilder()
                .setDeprecatedStartUtilization(0.2f)
                .setDeprecatedEndUtilization(0.4f).build())
            .build();

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(topologyEntityDTOList(Lists.newArrayList(
            new TestEntity(ENTITY_TO_RESIZE_NAME, targetId, EntityType.VIRTUAL_MACHINE_VALUE),
            new TestEntity(DC1_NAME, DATACENTER1_ID, EntityType.DATACENTER_VALUE),
            new TestEntity(DC2_NAME, DATACENTER2_ID, EntityType.DATACENTER_VALUE))));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(targetId)))
            .thenReturn(req);

        ActionSpec actionSpecTemplate = buildActionSpec(resizeInfo, resize);
        ActionSpec resizeActionSpec = actionSpecTemplate.toBuilder()
            .setRecommendationId(ACTION_STABLE_IMPACT_ID)
            .setRecommendation(actionSpecTemplate.getRecommendation().toBuilder()
                .setId(ACTION_LEGACY_INSTANCE_ID)
                .build())
            .build();

        final ActionApiDTO stableEnabledButPlanMarket =
            mapperWithStableIdEnabled.mapActionSpecToActionApiDTO(resizeActionSpec, CONTEXT_ID);
        assertEquals(String.valueOf(ACTION_LEGACY_INSTANCE_ID),
            stableEnabledButPlanMarket.getUuid());
        assertEquals(ACTION_LEGACY_INSTANCE_ID,
            stableEnabledButPlanMarket.getActionID().longValue());
        assertEquals(ACTION_STABLE_IMPACT_ID,
            stableEnabledButPlanMarket.getActionImpactID().longValue());

        final ActionApiDTO stableEnabledAndLiveMarket =
            mapperWithStableIdEnabled.mapActionSpecToActionApiDTO(resizeActionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID);
        // Both uuid and ActionID need to be the stable id when the feature flag is enabled and
        // the action is from the realtime market.
        assertEquals(String.valueOf(ACTION_STABLE_IMPACT_ID),
            stableEnabledAndLiveMarket.getUuid());
        assertEquals(ACTION_STABLE_IMPACT_ID,
            stableEnabledAndLiveMarket.getActionID().longValue());
        assertEquals(ACTION_STABLE_IMPACT_ID,
            stableEnabledAndLiveMarket.getActionImpactID().longValue());

        final ActionApiDTO stableDisabledAndLiveMarket =
            mapper.mapActionSpecToActionApiDTO(resizeActionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertEquals(String.valueOf(ACTION_LEGACY_INSTANCE_ID),
            stableDisabledAndLiveMarket.getUuid());
        assertEquals(ACTION_LEGACY_INSTANCE_ID,
            stableDisabledAndLiveMarket.getActionID().longValue());
        assertEquals(ACTION_STABLE_IMPACT_ID,
            stableDisabledAndLiveMarket.getActionImpactID().longValue());

    }

        /**
     * Test to verify the DiscoveredEntiyApiDTO is used to populate currentLocation and newLocation in the ActionApiDTO.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testDiscoveredEntityApiDTOIsInUse() throws Exception {
        final long targetId = 1;
        final ActionInfo resizeInfo = ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                    .setTarget(ApiUtilsTest.createActionEntity(targetId))
                    .setOldCapacity(9)
                    .setNewCapacity(10)
                    .setCommodityType(CPU.getCommodityType()))
            .build();

        Explanation resize = Explanation.newBuilder()
            .setResize(ResizeExplanation.newBuilder()
                .setDeprecatedStartUtilization(0.2f)
                .setDeprecatedEndUtilization(0.4f).build())
            .build();

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(topologyEntityDTOList(Lists.newArrayList(
                new TestEntity(ENTITY_TO_RESIZE_NAME, targetId, EntityType.VIRTUAL_MACHINE_VALUE),
                new TestEntity(DC1_NAME, DATACENTER1_ID, EntityType.DATACENTER_VALUE),
                new TestEntity(DC2_NAME, DATACENTER2_ID, EntityType.DATACENTER_VALUE))));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(targetId)))
            .thenReturn(req);

        final ActionApiDTO actionApiDTO =
                mapperWithDiscoveredEntityApiDTOEnabled.mapActionSpecToActionApiDTO(
                        buildActionSpec(resizeInfo, resize), CONTEXT_ID);

        // Verify that we set the context ID on the request.
        verify(req).contextId(CONTEXT_ID);

        assertEquals(ENTITY_TO_RESIZE_NAME, actionApiDTO.getTarget().getDisplayName());
        assertEquals(targetId, Long.parseLong(actionApiDTO.getTarget().getUuid()));

        assertEquals(DC1_NAME, actionApiDTO.getCurrentLocation().getDisplayName());
        assertEquals(DC1_NAME, actionApiDTO.getNewLocation().getDisplayName());

        assertTrue(actionApiDTO.getCurrentLocation() instanceof DiscoveredEntityApiDTO);
        assertTrue(actionApiDTO.getNewLocation() instanceof DiscoveredEntityApiDTO);
    }


    /**
     * Tests proper translation of action severity from the internal
     * XL format to the API action class.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testActionSeverityXlToApi() throws Exception {
        final long targetId = 1L;
        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
                topologyEntityDTO("EntityToActivate", targetId, EntityType.VIRTUAL_MACHINE_VALUE)));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(targetId)))
                .thenReturn(req);
        final Activate activate = Activate.newBuilder()
                                    .setTarget(ApiUtilsTest.createActionEntity(
                                                    targetId, EntityType.VIRTUAL_MACHINE_VALUE))
                                    .build();
        final Action action = Action.newBuilder()
                                .setId(0)
                                .setDeprecatedImportance(0.0d)
                                .setExplanation(Explanation.getDefaultInstance())
                                .setInfo(ActionInfo.newBuilder()
                                                .setActivate(activate))
                                .build();
        final ActionSpec actionSpec = ActionSpec.newBuilder()
                                            .setSeverity(Severity.CRITICAL)
                                            .setRecommendation(action)
                                            .build();
        assertEquals(ActionSpecMapper.mapSeverityToApi(Severity.CRITICAL),
                     mapper.mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID)
                        .getRisk()
                        .getSeverity());
    }

    @Test
    public void testMapActivate() throws Exception {
        final long targetId = 1;
        final ActionInfo activateInfo = ActionInfo.newBuilder()
                        .setActivate(Activate.newBuilder().setTarget(ApiUtilsTest.createActionEntity(targetId))
                                        .addTriggeringCommodities(CPU.getCommodityType())
                                        .addTriggeringCommodities(MEM.getCommodityType()))
                        .build();
        Explanation activate =
                        Explanation.newBuilder()
                                        .setActivate(ActivateExplanation.newBuilder()
                                                        .setMostExpensiveCommodity(CPU.getCommodityType()
                                                                                   .getType()).build())
                                        .build();
        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(topologyEntityDTOList(Lists.newArrayList(
            new TestEntity("EntityToActivate", targetId, EntityType.VIRTUAL_MACHINE_VALUE),
                new TestEntity(DC1_NAME, DATACENTER1_ID, EntityType.DATACENTER_VALUE))));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(targetId)))
            .thenReturn(req);

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
                buildActionSpec(activateInfo, activate), CONTEXT_ID);
        // Verify that we set the context ID on the request.
        verify(req).contextId(CONTEXT_ID);

        assertEquals("EntityToActivate", actionApiDTO.getTarget().getDisplayName());
        assertEquals(targetId, Long.parseLong(actionApiDTO.getTarget().getUuid()));
        assertEquals(ActionType.START, actionApiDTO.getActionType());
        Assert.assertEquals(ImmutableSet.of(UICommodityType.CPU.apiStr(), UICommodityType.MEM.apiStr()),
            actionApiDTO.getRisk().getReasonCommodities());
        assertEquals(DC1_NAME, actionApiDTO.getCurrentLocation().getDisplayName());
        assertNull(actionApiDTO.getNewLocation());
    }

    /**
     * Similar to 6.1, if entity is Storage, then it's ADD_PROVIDER.
     *
     * @throws Exception
     */
    @Test
    public void testMapStorageActivate() throws Exception {
        final long targetId = 1;
        final ActionInfo deactivateInfo = ActionInfo.newBuilder()
                .setActivate(Activate.newBuilder().setTarget(ApiUtilsTest.createActionEntity(targetId))
                        .addTriggeringCommodities(CPU.getCommodityType())
                        .addTriggeringCommodities(MEM.getCommodityType()))
                .build();
        Explanation activate = Explanation.newBuilder()
                .setDeactivate(DeactivateExplanation.newBuilder().build()).build();
        final String entityToActivateName = "EntityToActivate";
        final String prettyClassName = "Storage";

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(topologyEntityDTOList(Lists.newArrayList(
                new TestEntity(entityToActivateName, targetId, EntityType.STORAGE_VALUE),
                new TestEntity(DC1_NAME, DATACENTER1_ID, EntityType.DATACENTER_VALUE))));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(targetId)))
            .thenReturn(req);

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
                buildActionSpec(deactivateInfo, activate), CONTEXT_ID);
        // Verify that we set the context ID on the request.
        verify(req).contextId(CONTEXT_ID);

        assertEquals(entityToActivateName, actionApiDTO.getTarget().getDisplayName());
        assertEquals(targetId, Long.parseLong(actionApiDTO.getTarget().getUuid()));
        assertEquals(ActionType.START, actionApiDTO.getActionType());
        assertEquals(ImmutableSet.of(UICommodityType.CPU.apiStr(), UICommodityType.MEM.apiStr()),
            actionApiDTO.getRisk().getReasonCommodities());
        assertEquals(DC1_NAME, actionApiDTO.getCurrentLocation().getDisplayName());
        assertNull(actionApiDTO.getNewLocation());
    }

    @Test
    public void testMapDeactivate() throws Exception {
        final long targetId = 1;
        final ActionInfo deactivateInfo = ActionInfo.newBuilder()
            .setDeactivate(Deactivate.newBuilder().setTarget(ApiUtilsTest.createActionEntity(targetId))
                .addTriggeringCommodities(CPU.getCommodityType())
                .addTriggeringCommodities(MEM.getCommodityType()))
            .build();
        Explanation deactivate = Explanation.newBuilder()
            .setDeactivate(DeactivateExplanation.newBuilder().build()).build();
        final String entityToDeactivateName = "EntityToDeactivate";

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(topologyEntityDTOList(Lists.newArrayList(
            new TestEntity(entityToDeactivateName, targetId, EntityType.VIRTUAL_MACHINE_VALUE),
                new TestEntity(DC1_NAME, DATACENTER1_ID, EntityType.DATACENTER_VALUE))));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(targetId)))
            .thenReturn(req);

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
            buildActionSpec(deactivateInfo, deactivate), CONTEXT_ID);
        // Verify that we set the context ID on the request.
        verify(req).contextId(CONTEXT_ID);

        assertEquals(entityToDeactivateName, actionApiDTO.getTarget().getDisplayName());
        assertEquals(targetId, Long.parseLong(actionApiDTO.getTarget().getUuid()));
        assertEquals(ActionType.SUSPEND, actionApiDTO.getActionType());
        assertEquals(ImmutableSet.of(UICommodityType.CPU.apiStr(), UICommodityType.MEM.apiStr()),
            actionApiDTO.getRisk().getReasonCommodities());
        assertEquals(DC1_NAME, actionApiDTO.getCurrentLocation().getDisplayName());
        assertNull(actionApiDTO.getNewLocation());
    }

    @Test
    public void testMapDeactivateContainer() throws Exception {
        final long targetId = 1;
        ActionEntity ae = ActionEntity.newBuilder()
                .setId(targetId)
                .setType(EntityType.CONTAINER_VALUE)
                .build();
        final ActionInfo deactivateInfo = ActionInfo.newBuilder()
                .setDeactivate(Deactivate.newBuilder().setTarget(ae)
                        .addTriggeringCommodities(CPU.getCommodityType())
                        .addTriggeringCommodities(MEM.getCommodityType()))
                .build();
        Explanation deactivate = Explanation.newBuilder()
                .setDeactivate(DeactivateExplanation.newBuilder().build()).build();
        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
                buildActionSpec(deactivateInfo, deactivate), CONTEXT_ID);
        assertEquals(targetId, Long.parseLong(actionApiDTO.getTarget().getUuid()));
        assertEquals(ActionType.SUSPEND, actionApiDTO.getActionType());
        //For Container or Container Pod, we should not populate the reasonCommodities
        assertNull(actionApiDTO.getRisk().getReasonCommodities());
    }

    @Test
    public void testMapDelete() throws Exception {
        final long targetId = 1;
        final String fileName = "foobar";
        final String filePath = "/etc/local/" + fileName;
        final long modificationTime = 25000L;
        final ActionInfo deleteInfo = ActionInfo.newBuilder()
            .setDelete(Delete.newBuilder().setTarget(ApiUtilsTest.createActionEntity(targetId))
                .setFilePath(filePath)
                .build())
            .build();
        Explanation delete = Explanation.newBuilder()
            .setDelete(DeleteExplanation.newBuilder().setSizeKb(2048L)
                .setModificationTimeMs(modificationTime)
                .build()).build();
        final String entityToDelete = "EntityToDelete";

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO(entityToDelete, targetId, EntityType.STORAGE_VALUE)));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(targetId)))
            .thenReturn(req);

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
                buildActionSpecWithDisruptiveReversible(deleteInfo, delete, false, false), CONTEXT_ID);
        // Verify that we set the context ID on the request.
        verify(req).contextId(CONTEXT_ID);

        assertEquals(entityToDelete, actionApiDTO.getTarget().getDisplayName());
        assertEquals(targetId, Long.parseLong(actionApiDTO.getTarget().getUuid()));
        assertEquals(ActionType.DELETE, actionApiDTO.getActionType());
        assertEquals(1, actionApiDTO.getVirtualDisks().size());
        assertEquals(filePath, actionApiDTO.getVirtualDisks().get(0).getDisplayName());
        assertEquals(modificationTime, actionApiDTO.getVirtualDisks().get(0).getLastModified());
        assertEquals("2.0", actionApiDTO.getCurrentValue());
        assertEquals("MB", actionApiDTO.getValueUnits());
        assertNull(actionApiDTO.getRisk().getReasonCommodities());
        // Validate actionExecutionCharacteristics
        ActionExecutionCharacteristicApiDTO actionExecutionCharacteristics
                = actionApiDTO.getExecutionCharacteristics();
        assertNotNull(actionExecutionCharacteristics);
        assertEquals(ActionDisruptiveness.NON_DISRUPTIVE, actionExecutionCharacteristics.getDisruptiveness());
        assertEquals(ActionReversibility.IRREVERSIBLE, actionExecutionCharacteristics.getReversibility());
    }

    /**
     * Similar to 6.1, if entity is Disk Array, then it's DELETE.
     *
     * @throws Exception
     */
    @Test
    public void testMapDiskArrayDeactivate() throws Exception {
        final long targetId = 1;
        final ActionInfo deactivateInfo = ActionInfo.newBuilder()
                .setDeactivate(Deactivate.newBuilder().setTarget(ApiUtilsTest.createActionEntity(targetId))
                        .addTriggeringCommodities(CPU.getCommodityType())
                        .addTriggeringCommodities(MEM.getCommodityType()))
                .build();
        Explanation deactivate = Explanation.newBuilder()
                .setDeactivate(DeactivateExplanation.newBuilder().build()).build();
        final String entityToDeactivateName = "EntityToDeactivate";
        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO(entityToDeactivateName, targetId, EntityType.DISK_ARRAY_VALUE)));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(targetId)))
            .thenReturn(req);

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
                buildActionSpec(deactivateInfo, deactivate), CONTEXT_ID);
        // Verify that we set the context ID on the request.
        verify(req).contextId(CONTEXT_ID);

        assertEquals(entityToDeactivateName, actionApiDTO.getTarget().getDisplayName());
        assertEquals(targetId, Long.parseLong(actionApiDTO.getTarget().getUuid()));
        assertEquals(ActionType.SUSPEND, actionApiDTO.getActionType());
        assertEquals(ImmutableSet.of(UICommodityType.CPU.apiStr(), UICommodityType.MEM.apiStr()),
            actionApiDTO.getRisk().getReasonCommodities());

        assertEquals(ImmutableSet.of("Mem", "CPU"), actionApiDTO.getRisk().getReasonCommodities());
    }

    @Test
    public void testUpdateTime() throws Exception {

        // Arrange
        ActionInfo moveInfo = getHostMoveActionInfo();
        ActionDTO.ActionDecision decision = ActionDTO.ActionDecision.newBuilder()
                        .setDecisionTime(System.currentTimeMillis()).build();
        String expectedUpdateTime = DateTimeUtil.toString(decision.getDecisionTime());
        Explanation placement = Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setInitialPlacement(InitialPlacement.newBuilder())))
                .build();
        final ActionSpec actionSpec = buildActionSpec(moveInfo, placement,
                Optional.of(decision), Optional.empty(), null);
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);

        // Act
        ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID);

        // Assert
        assertThat(actionApiDTO.getUpdateTime(), is(expectedUpdateTime));
    }



    @Test
    public void testPlacementPolicyMove() throws Exception {
        final ActionInfo moveInfo = ActionInfo.newBuilder().setMove(Move.newBuilder()
                        .setTarget(ApiUtilsTest.createActionEntity(1))
                        .addChanges(ChangeProvider.newBuilder()
                                        .setSource(ApiUtilsTest.createActionEntity(2))
                                        .setDestination(ApiUtilsTest.createActionEntity(3))
                                        .build())
                        .build())
                        .build();

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(topologyEntityDTOList(Lists.newArrayList(
            new TestEntity("target", 1, EntityType.VIRTUAL_MACHINE_VALUE),
                new TestEntity("source", 2, EntityType.VIRTUAL_MACHINE_VALUE),
                new TestEntity("dest", 3, EntityType.VIRTUAL_MACHINE_VALUE))));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(1L, 2L, 3L)))
            .thenReturn(req);

        final Compliance compliance = Compliance.newBuilder()
                        .addMissingCommodities(createReasonCommodity(CommodityDTO.CommodityType.SEGMENTATION_VALUE,
                                                                     String.valueOf(POLICY_ID)))
                        .build();
        final MoveExplanation moveExplanation = MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                        .setCompliance(compliance).build())
                        .build();
        final List<ActionApiDTO> dtos = mapper.mapActionSpecsToActionApiDTOs(
                        Arrays.asList(buildActionSpec(moveInfo, Explanation.newBuilder()
                                        .setMove(moveExplanation).build())),
                        REAL_TIME_TOPOLOGY_CONTEXT_ID);
        // Verify that we set the context ID on the request.
        verify(req).contextId(REAL_TIME_TOPOLOGY_CONTEXT_ID);

        Assert.assertEquals("\"target\" doesn't comply with \"" + POLICY_NAME + "\"",
                        dtos.get(0).getRisk().getDescription());
        assertEquals(ImmutableSet.of("SegmentationCommodity"), dtos.get(0).getRisk().getReasonCommodities());
    }

    @Test
    public void testPlacementPolicyCompoundMove() throws Exception {
        ActionEntity vm = ApiUtilsTest.createActionEntity(1, EntityType.VIRTUAL_MACHINE_VALUE);
        final ActionInfo compoundMoveInfo = ActionInfo.newBuilder().setMove(Move.newBuilder()
            .setTarget(vm)
            .addChanges(ChangeProvider.newBuilder()
                .setSource(ApiUtilsTest.createActionEntity(2, EntityType.PHYSICAL_MACHINE_VALUE))
                .setDestination(ApiUtilsTest.createActionEntity(3, EntityType.PHYSICAL_MACHINE_VALUE))
                .build())
            .addChanges(ChangeProvider.newBuilder()
                .setSource(ApiUtilsTest.createActionEntity(4, EntityType.STORAGE_VALUE))
                .setDestination(ApiUtilsTest.createActionEntity(5, EntityType.STORAGE_VALUE))
                .build()))
            .build();
        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(topologyEntityDTOList(Lists.newArrayList(
            new TestEntity("target", 1, EntityType.VIRTUAL_MACHINE_VALUE),
            new TestEntity("source", 2, EntityType.PHYSICAL_MACHINE_VALUE),
            new TestEntity("dest", 3, EntityType.PHYSICAL_MACHINE_VALUE),
            new TestEntity("stSource", 4, EntityType.STORAGE_VALUE),
            new TestEntity("stDest", 5, EntityType.STORAGE_VALUE))));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(1L, 2L, 3L, 4L, 5L)))
            .thenReturn(req);

        final Compliance compliance = Compliance.newBuilder()
                        .addMissingCommodities(createReasonCommodity(CommodityDTO.CommodityType.SEGMENTATION_VALUE,
                                                                     String.valueOf(POLICY_ID)))
                        .build();
        // Test that we use the policy name in explanation if the primary explanation is compliance.
        // We always go with the primary explanation if available
        final MoveExplanation moveExplanation1 = MoveExplanation.newBuilder()
            .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                .setPerformance(Performance.getDefaultInstance()).build())
            .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                .setCompliance(compliance).setIsPrimaryChangeProviderExplanation(true).build())
            .build();
        final List<ActionApiDTO> dtos1 = mapper.mapActionSpecsToActionApiDTOs(
            Arrays.asList(buildActionSpec(compoundMoveInfo, Explanation.newBuilder()
                .setMove(moveExplanation1).build())), CONTEXT_ID);
        Assert.assertEquals("\"target\" doesn't comply with \"" + POLICY_NAME + "\"",
                dtos1.get(0).getRisk().getDescription());
        assertEquals(ImmutableSet.of("SegmentationCommodity"), dtos1.get(0).getRisk().getReasonCommodities());

        // Test that we do not modify the explanation if the primary explanation is not compliance.
        // We always go with the primary explanation if available
        final MoveExplanation moveExplanation2 = MoveExplanation.newBuilder()
            .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                .setPerformance(Performance.getDefaultInstance())
                .setIsPrimaryChangeProviderExplanation(true).build())
            .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                .setCompliance(compliance).build())
            .build();
        final List<ActionApiDTO> dtos2 = mapper.mapActionSpecsToActionApiDTOs(
            Arrays.asList(buildActionSpec(compoundMoveInfo, Explanation.newBuilder()
                .setMove(moveExplanation2).build())), CONTEXT_ID);
        Assert.assertEquals(DEFAULT_EXPLANATION, dtos2.get(0).getRisk().getDescription());
        assertEquals(ImmutableSet.of("SegmentationCommodity"), dtos2.get(0).getRisk().getReasonCommodities());
        Assert.assertEquals(Collections.singletonList(DEFAULT_PRE_REQUISITE_DESCRIPTION),
            dtos2.get(0).getPrerequisites());
    }

    @Test
    public void testBuyRIWithoutRegion() throws Exception {
        ReservedInstanceApiDTO riApiDTO = new ReservedInstanceApiDTO();
        riApiDTO.setTemplate(createPolicyBaseApiDTO(1L, "default", "default"));
        riApiDTO.setPayment(PaymentOption.ALL_UPFRONT);
        riApiDTO.setPlatform(Platform.LINUX);
        riApiDTO.setTerm(mock(StatApiDTO.class));
        riApiDTO.setType(ReservedInstanceType.STANDARD);
        riApiDTO.setInstanceCount(1);
        when(reservedInstanceMapper.mapToReservedInstanceApiDTO(any(), any(), any(), any(), any(), any()))
            .thenReturn(riApiDTO);

        ReservedInstanceBought riBought = ReservedInstanceBought.newBuilder()
            .setId(1)
            .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                .setReservedInstanceSpec(1).build())
            .build();
        ReservedInstanceSpec spec = ReservedInstanceSpec.newBuilder().setId(1).build();
        Map<Long, Pair<ReservedInstanceBought, ReservedInstanceSpec>> map = new HashMap<>();
        map.put(1L, new Pair<>(riBought, spec));

        Map<Long, PolicyApiDTO> policyApiDto = new HashMap<>();
        Map<Long, ApiPartialEntity> entitiesMap = new HashMap<>();
        entitiesMap.put(1L, topologyEntityDTO("Test Entity", 1L, EntityType.RESERVED_INSTANCE_VALUE));
        Map< Long, BaseApiDTO> relatedSettingsPolicies = new HashMap<>();
        relatedSettingsPolicies.put(1L, createPolicyBaseApiDTO(1L, "Test1", "default"));

        ActionSpecMappingContext context = new ActionSpecMappingContext(entitiesMap, Collections.emptyMap(), Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), map,
                Collections.emptyMap(), serviceEntityMapper, false, policyApiDto, relatedSettingsPolicies, Collections.emptyMap());

        ActionSpecMappingContextFactory contextFactory = mock(ActionSpecMappingContextFactory.class);
        when(contextFactory.getBuyRIIdToRIBoughtandRISpec(any())).thenReturn(map);
        when(contextFactory.createActionSpecMappingContext(any(), anyLong(), any())).thenReturn(context);

        ActionSpecMapper actionMapper = new ActionSpecMapper(
            contextFactory,
            reservedInstanceMapper,
            RIBuyContextFetchServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            CostServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            ReservedInstanceUtilizationCoverageServiceGrpc.newBlockingStub(grpcServer.getChannel()),
            mock(BuyRiScopeHandler.class),
            REAL_TIME_TOPOLOGY_CONTEXT_ID,
            uuidMapper,
            cloudSavingsDetailsDtoConverter,
            groupExpander,
            true);

        ActionInfo buyInfo = ActionInfo.newBuilder()
            .setBuyRi(BuyRI.newBuilder()
                .setBuyRiId(1L)
                .setMasterAccount(ActionEntity.newBuilder()
                    .setId(1L)
                    .setType(EntityType.BUSINESS_ACCOUNT_VALUE)
                    .build())
                .build())
            .build();

        Explanation explanation = Explanation.newBuilder()
            .setBuyRI(BuyRIExplanation.newBuilder()
                .setCoveredAverageDemand(5f)
                .setTotalAverageDemand(10f)
                .build())
            .build();
        
        List<ActionApiDTO> dtos = actionMapper.mapActionSpecsToActionApiDTOs(
            Arrays.asList(buildActionSpec(buyInfo, explanation)),
            REAL_TIME_TOPOLOGY_CONTEXT_ID);
    }


    /**
     * To align with classic, plan action should have succeeded state, so it's not selectable from UI.
     */
    @Test
    public void testToPlanAction() throws Exception {
        final ActionInfo moveInfo = getHostMoveActionInfo();
        final Explanation compliance = Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setCompliance(Compliance.newBuilder()
                                        .addMissingCommodities(MEM)
                                        .addMissingCommodities(CPU)
                                        .build())
                                .build())
                        .build())
                .build();
        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO("target", 1, EntityType.VIRTUAL_MACHINE_VALUE),
            topologyEntityDTO("source", 2, EntityType.VIRTUAL_MACHINE_VALUE),
            topologyEntityDTO("dest", 3, EntityType.VIRTUAL_MACHINE_VALUE)));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(1L, 2L, 3L)))
            .thenReturn(req);
        final MultiEntityRequest fullReq = ApiTestUtils.mockMultiEntityReqEmpty();
        when(repositoryApi.entitiesRequest(Collections.emptySet())).thenReturn(fullReq);

        final ActionApiDTO actionApiDTO =
                mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, compliance), CONTEXT_ID);
        assertEquals(com.vmturbo.api.enums.ActionState.SUCCEEDED, actionApiDTO.getActionState());

        final ActionApiDTO realTimeActionApiDTO =
                mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, compliance), REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertEquals(com.vmturbo.api.enums.ActionState.READY, realTimeActionApiDTO.getActionState());
    }

    @Test
    public void testMapReadyRecommendModeExecutable() throws Exception {
        Explanation placement = Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setInitialPlacement(InitialPlacement.newBuilder())))
                .build();
        final ActionSpec actionSpec = buildActionSpec(getHostMoveActionInfo(), placement).toBuilder()
            // The action is in READY state, and in RECOMMEND mode.
                .setActionState(ActionDTO.ActionState.READY)
            .setActionMode(ActionMode.RECOMMEND)
            .build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertThat(actionApiDTO.getActionState(), is(com.vmturbo.api.enums.ActionState.READY));
    }

    @Test
    public void testMapReadyRecommendModeNotExecutable() throws Exception {
        Explanation placement = Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setInitialPlacement(InitialPlacement.newBuilder())))
                .build();
        final ActionSpec actionSpec = buildActionSpec(getHostMoveActionInfo(), placement).toBuilder()
                .setActionState(ActionDTO.ActionState.READY)
            .setActionMode(ActionMode.RECOMMEND)
            .setIsExecutable(false)
            .build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertThat(actionApiDTO.getActionState(), is(com.vmturbo.api.enums.ActionState.READY));
    }

    @Test
    public void testMapReadyNotRecommendModeExecutable() throws Exception {
        Explanation placement = Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setInitialPlacement(InitialPlacement.newBuilder())))
                .build();
        final ActionSpec actionSpec = buildActionSpec(getHostMoveActionInfo(), placement).toBuilder()
            // The action is in READY state, and in RECOMMEND mode.
                .setActionState(ActionDTO.ActionState.READY)
            .setActionMode(ActionMode.MANUAL)
            .build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertThat(actionApiDTO.getActionState(), is(com.vmturbo.api.enums.ActionState.READY));
    }

    @Test
    public void testMapReadyNotRecommendModeNotExecutable() throws Exception {
        Explanation placement = Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setInitialPlacement(InitialPlacement.newBuilder())))
                .build();
        final ActionSpec actionSpec = buildActionSpec(getHostMoveActionInfo(), placement).toBuilder()
                .setActionState(ActionDTO.ActionState.READY)
            .setActionMode(ActionMode.MANUAL)
            .setIsExecutable(false)
            .build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertThat(actionApiDTO.getActionState(), is(com.vmturbo.api.enums.ActionState.READY));
    }

    @Test
    public void testMapNotReadyRecommendModeExecutable() throws Exception {
        Explanation placement = Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setInitialPlacement(InitialPlacement.newBuilder())))
                .build();
        final ActionSpec actionSpec = buildActionSpec(getHostMoveActionInfo(), placement).toBuilder()
                .setActionState(ActionDTO.ActionState.QUEUED)
            .setActionMode(ActionMode.RECOMMEND)
            .build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertThat(actionApiDTO.getActionState(), is(com.vmturbo.api.enums.ActionState.QUEUED));
    }

    @Test
    public void testCreateActionFilterNoInvolvedEntities() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();

        final ActionQueryFilter filter = createFilter(inputDto);

        assertFalse(filter.hasInvolvedEntities());
    }

    @Test
    public void testCreateActionFilterDates() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        inputDto.setStartTime(DateTimeUtil.toString(1_000_000));
        inputDto.setEndTime(DateTimeUtil.toString(2_000_000));

        final ActionQueryFilter filter = createFilter(inputDto);
        assertThat(filter.getStartDate(), is(1_000_000L));
        assertThat(filter.getEndDate(), is(2_000_000L));
    }

    /**
     * Test action filtering for actiond description.
     */
    @Test
    public void testCreateActionFilterDescriptionQuery() {
        final ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        final QueryInputApiDTO descriptionQuery = new QueryInputApiDTO();
        descriptionQuery.setQuery("test");
        descriptionQuery.setType(QueryType.CONTAINS);
        inputDTO.setDescriptionQuery(descriptionQuery);

        final ActionQueryFilter filter = createFilter(inputDTO);
        assertEquals("(?i)(.*\\Qtest\\E.*)", filter.getDescriptionQuery());
    }

    /**
     * Test action filtering for disruptiveness.
     */
    @Test
    public void testCreateActionFilterDisruptiveness() {
        final ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        final ActionExecutionCharacteristicInputApiDTO executionCharacteristicInputApiDTO = new ActionExecutionCharacteristicInputApiDTO();
        executionCharacteristicInputApiDTO.setDisruptiveness(ActionDisruptiveness.DISRUPTIVE);
        inputDTO.setExecutionCharacteristics(executionCharacteristicInputApiDTO);

        final ActionQueryFilter filter = createFilter(inputDTO);
        assertSame(ActionDTO.ActionDisruptiveness.DISRUPTIVE, filter.getDisruptiveness());

        final ActionExecutionCharacteristicInputApiDTO executionCharacteristicInputApiDTO2 = new ActionExecutionCharacteristicInputApiDTO();
        executionCharacteristicInputApiDTO2.setDisruptiveness(ActionDisruptiveness.NON_DISRUPTIVE);
        inputDTO.setExecutionCharacteristics(executionCharacteristicInputApiDTO2);

        final ActionQueryFilter filter2 = createFilter(inputDTO);
        assertSame(ActionDTO.ActionDisruptiveness.NON_DISRUPTIVE, filter2.getDisruptiveness());
    }

    /**
     * Test action filtering for reversibility.
     */
    @Test
    public void testCreateActionFilterReversibility() {
        final ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        final ActionExecutionCharacteristicInputApiDTO executionCharacteristicInputApiDTO = new ActionExecutionCharacteristicInputApiDTO();
        executionCharacteristicInputApiDTO.setReversibility(ActionReversibility.REVERSIBLE);
        inputDTO.setExecutionCharacteristics(executionCharacteristicInputApiDTO);

        final ActionQueryFilter filter = createFilter(inputDTO);
        assertSame(ActionDTO.ActionReversibility.REVERSIBLE, filter.getReversibility());

        final ActionExecutionCharacteristicInputApiDTO executionCharacteristicInputApiDTO2 = new ActionExecutionCharacteristicInputApiDTO();
        executionCharacteristicInputApiDTO2.setReversibility(ActionReversibility.IRREVERSIBLE);
        inputDTO.setExecutionCharacteristics(executionCharacteristicInputApiDTO2);

        final ActionQueryFilter filter2 = createFilter(inputDTO);
        assertSame(ActionDTO.ActionReversibility.IRREVERSIBLE, filter2.getReversibility());
    }

    /**
     * Test action filtering for savings amount.
     */
    @Test
    public void testCreateActionFilterSaingsAmount() {
        final ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        final RangeInputApiDTO rangeInputApiDTO = new RangeInputApiDTO();
        rangeInputApiDTO.setMaxValue(20.0f);
        rangeInputApiDTO.setMinValue(10.0f);
        inputDTO.setSavingsAmountRange(rangeInputApiDTO);

        final ActionQueryFilter filter = createFilter(inputDTO);
        assertEquals(20, filter.getSavingsAmountRange().getMaxValue(), 0);
        assertEquals(10, filter.getSavingsAmountRange().getMinValue(), 0);
    }

    /**
     * Test action filtering for action with schedule.
     */
    @Test
    public void testCreateActionFilterHasSchedule() {
        final ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        inputDTO.setHasSchedule(true);

        final ActionQueryFilter filter = createFilter(inputDTO);
        assertTrue(filter.getHasSchedule());
    }

    /**
     * Test action filtering for action has prerequisites.
     */
    @Test
    public void testCreateActionFilterHasPrerequisites() {
        final ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        inputDTO.setHasPrerequisites(true);

        final ActionQueryFilter filter = createFilter(inputDTO);
        assertTrue(filter.getHasPrerequisites());
    }

    /**
     * Test that getting actions by market UUID fails if given future start time.
     *
     * @throws Exception should throw {@link IllegalArgumentException}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetActionsByMarketUuidShouldFailGivenFutureStartTime() throws Exception {
        final ActionApiInputDTO actionDTO = new ActionApiInputDTO();
        final Long currentDateTime = DateTimeUtil.parseTime(DateTimeUtil.getNow());
        final String futureDate = DateTimeUtil.addDays(currentDateTime, 2);
        actionDTO.setStartTime(futureDate);
        createFilter(actionDTO);
    }

    /**
     * Test that getting actions by market UUID fails if given start time after end time.
     *
     * @throws Exception should throw {@link IllegalArgumentException}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetActionsByMarketUuidShouldFailGivenStartTimeAfterEndTime() throws Exception {
        ActionApiInputDTO actionDTO = new ActionApiInputDTO();
        String currentDateTime = DateTimeUtil.getNow();
        String futureDate = DateTimeUtil.addDays(DateTimeUtil.parseTime(currentDateTime), 2);
        actionDTO.setStartTime(futureDate);
        actionDTO.setEndTime(currentDateTime);
        createFilter(actionDTO);
    }

    /**
     * Test that getting actions by market UUID fails if given end time and no start time.
     *
     * @throws Exception should throw {@link IllegalArgumentException}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetActionsByMarketUuidShouldFailGivenEndTimeOnly() throws Exception {
        ActionApiInputDTO actionDTO = new ActionApiInputDTO();
        actionDTO.setEndTime(DateTimeUtil.getNow());
        createFilter(actionDTO);
    }

    @Test
    public void testCreateActionFilterEnvTypeCloud() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        inputDto.setEnvironmentType(EnvironmentType.CLOUD);

        final ActionQueryFilter filter = createFilter(inputDto);
        assertThat(filter.getEnvironmentType(), is(EnvironmentTypeEnum.EnvironmentType.CLOUD));
    }

    @Test
    public void testCreateActionFilterEnvTypeOnPrem() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        inputDto.setEnvironmentType(EnvironmentType.ONPREM);

        final ActionQueryFilter filter = createFilter(inputDto);
        assertThat(filter.getEnvironmentType(), is(EnvironmentTypeEnum.EnvironmentType.ON_PREM));
    }

    @Test
    public void testCreateActionFilterEnvTypeUnset() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();

        final ActionQueryFilter filter = createFilter(inputDto);
        assertFalse(filter.hasEnvironmentType());
    }

    @Test
    public void testCreateActionFilterWithInvolvedEntities() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        final Set<Long> oids = Sets.newHashSet(1L, 2L, 3L);
        final Optional<Set<Long>> involvedEntities = Optional.of(oids);

        final ActionQueryFilter filter = mapper.createActionFilter(inputDto, involvedEntities, null);

        assertTrue(filter.hasInvolvedEntities());
        assertEquals(new HashSet<>(oids),
                     new HashSet<>(filter.getInvolvedEntities().getOidsList()));
    }

    @Test
    public void testCreateActionFilterWithBuyRis() {
        final ActionQueryFilter filter = mapper.createActionFilter(emptyInputDto, Optional.empty(),
                scopeWithBuyRiActions);

        assertTrue(filter.hasInvolvedEntities());
        assertEquals(buyRiOids, new HashSet<>(filter.getInvolvedEntities().getOidsList()));
        assertEquals(buyRiActionTypes, new HashSet<>(filter.getTypesList()));
    }

    @Test
    public void testCreateActionFilterWithInvolvedEntitiesAndBuyRis() {
        final Set<Long> involvedEntities = ImmutableSet.of(1L, 2L);

        final ActionQueryFilter filter = mapper.createActionFilter(emptyInputDto,
                Optional.of(involvedEntities), scopeWithBuyRiActions);

        assertTrue(filter.hasInvolvedEntities());
        assertEquals(Sets.union(buyRiOids, involvedEntities),
                new HashSet<>(filter.getInvolvedEntities().getOidsList()));
    }

    /**
     * Tests that a list of API strings representing severity filters
     * is translated correctly into an internal XL filter.
     */
    @Test
    public void testCreateActionFilterWithSeverities() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        inputDto.setRiskSeverityList(ImmutableList.of("Critical", "MAJOR"));
        final ActionQueryFilter filter = mapper.createActionFilter(inputDto, Optional.empty(), null);
        Assert.assertThat(filter.getSeveritiesList(), containsInAnyOrder(Severity.CRITICAL, Severity.MAJOR));
    }

    /**
     * Test that the related entity types from {@link ActionApiInputDTO} makes its way into
     * the mapped {@link ActionQueryFilter}.
     */
    @Test
    public void testCreateActionFilterWithInvolvedEntityTypes() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        inputDto.setRelatedEntityTypes(Arrays.asList(ApiEntityType.VIRTUAL_MACHINE.apiStr(),
            ApiEntityType.PHYSICAL_MACHINE.apiStr()));

        final ActionQueryFilter filter = createFilter(inputDto);

        assertThat(filter.getEntityTypeList(),
            containsInAnyOrder(ApiEntityType.VIRTUAL_MACHINE.typeNumber(),
                ApiEntityType.PHYSICAL_MACHINE.typeNumber()));
    }


    // The UI request for "Pending Actions" does not include any action states
    // in its filter even though it wants to exclude executed actions. When given
    // no action states we should automatically insert the operational action states.
    @Test
    public void testCreateActionFilterWithNoStateFilter() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();

        final ActionQueryFilter filter = createFilter(inputDto);
        Assert.assertThat(filter.getStatesList(),
            containsInAnyOrder(ActionSpecMapper.OPERATIONAL_ACTION_STATES.toArray()));
    }

    // Similar fixes as in OM-24590: Do not show executed actions as pending,
    // when "inputDto" is null, we should automatically insert the operational action states.
    @Test
    public void testCreateActionFilterWithNoStateFilterAndNoInputDTO() {
        final ActionQueryFilter filter = createFilter(null);
        Assert.assertThat(filter.getStatesList(),
                containsInAnyOrder(ActionSpecMapper.OPERATIONAL_ACTION_STATES.toArray()));
    }

    @Test
    public void testMapClearedState() {
        Optional<ActionDTO.ActionState> state = ActionSpecMapper.mapApiStateToXl(com.vmturbo.api.enums.ActionState.CLEARED);
        assertThat(state.get(), is(ActionDTO.ActionState.CLEARED));
    }

    @Test (expected = IllegalArgumentException.class)
    public void testRejectedStateMustReturnError() {
        createActionFilterMustFail(com.vmturbo.api.enums.ActionState.REJECTED);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testAccountingStateMustReturnError() {
        createActionFilterMustFail(com.vmturbo.api.enums.ActionState.ACCOUNTING);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testPendingAcceptStateMustReturnError() {
        createActionFilterMustFail(com.vmturbo.api.enums.ActionState.PENDING_ACCEPT);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testRecommendedStateMustReturnError() {
        createActionFilterMustFail(com.vmturbo.api.enums.ActionState.RECOMMENDED);
    }

    /**
     * A template for testing passing unsupported action states to createActionFilter method.
     * The expected behavior is that the method should throw an IllegalArgumentException.
     *
     * @param state The unsupported ActionState
     */
    private void createActionFilterMustFail(com.vmturbo.api.enums.ActionState state) {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        List<com.vmturbo.api.enums.ActionState> actionStates = new ArrayList<>();
        actionStates.add(state);
        inputDto.setActionStateList(actionStates);
        createFilter(inputDto);
    }

    @Test
    public void testMapPostInProgress() {
        ActionSpec.Builder builder = ActionSpec.newBuilder()
                .setActionState(ActionDTO.ActionState.POST_IN_PROGRESS);
        ActionSpec actionSpec = builder.build();
        com.vmturbo.api.enums.ActionState actionState = mapXlActionStateToApi(actionSpec.getActionState());
        assertThat(actionState, is(com.vmturbo.api.enums.ActionState.IN_PROGRESS));
    }

    @Test
    public void testMapPreInProgress() {
        ActionSpec.Builder builder = ActionSpec.newBuilder()
                .setActionState(ActionDTO.ActionState.PRE_IN_PROGRESS);
        ActionSpec actionSpec = builder.build();
        com.vmturbo.api.enums.ActionState actionState = mapXlActionStateToApi(actionSpec.getActionState());
        assertThat(actionState, is(com.vmturbo.api.enums.ActionState.IN_PROGRESS));
    }

    /**
     * Test username displayed in UI.
     */
    @Test
    public void testGetUserName() {
        final String sampleUserUuid = "administrator(22222222222)";
        final String userName = "admin";
        assertEquals("administrator", mapper.getUserName(sampleUserUuid));
        assertEquals(AuditLogUtils.SYSTEM, mapper.getUserName(AuditLogUtils.SYSTEM));
        assertEquals(userName, mapper.getUserName(userName));
    }

    /**
     * Test that the creation of the ActionExecutionAuditApiDTO is not created, when the Action is not executed.
     */
    @Test
    public void testActionExecutionNull() throws Exception {
        final ActionInfo actionInfo = getHostMoveActionInfo();
        Explanation placement = Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setInitialPlacement(InitialPlacement.newBuilder())))
                .build();
        final ActionSpec actionSpec = ActionSpec.newBuilder()
                .setActionState(ActionDTO.ActionState.READY)
                .setRecommendation(buildAction(actionInfo, placement))
                .build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        final ActionExecutionAuditApiDTO executionDto = mapper
                .mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID, ActionDetailLevel.EXECUTION)
                .getExecutionStatus();

        // Assert
        assertNull(executionDto);
    }

    /**
     * Test to verify that the current and new entities of a given intra-tier scale action are set
     * to the same primary provider entity if available. If primary provider is not available,
     * verifies that current and new values are effectively unset.
     *
     * @throws Exception Potentially thrown by mapActionSpecToActionApiDTO
     */
    @Test
    public void testSameCurrentAndNewEntities() throws Exception {
        final int progress = 20;
        final ActionDTO.ExecutionStep executionStepProgress = ActionDTO.ExecutionStep.newBuilder()
                .setStartTime(MILLIS_2020_01_01_00_00_00)
                .setProgressPercentage(progress)
                .build();
        final ActionInfo actionInfoWithPrimaryProvider = getScaleWithinTierActionInfo(true);
        Explanation scaleExplanation = Explanation.newBuilder()
                .setScale(ScaleExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()))
                .build();
        ActionSpec scaleWithinTierWithPrimaryProviderActionSpec = ActionSpec.newBuilder()
                .setExecutionStep(executionStepProgress)
                .setActionState(ActionDTO.ActionState.IN_PROGRESS)
                .setRecommendation(buildAction(actionInfoWithPrimaryProvider, scaleExplanation))
                .build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        ActionApiDTO scaleWithinTierActionApiDTO = mapper
                .mapActionSpecToActionApiDTO(scaleWithinTierWithPrimaryProviderActionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID, ActionDetailLevel.EXECUTION);
        assertNotNull(scaleWithinTierActionApiDTO.getCurrentEntity());
        assertEquals(scaleWithinTierActionApiDTO.getCurrentEntity(), scaleWithinTierActionApiDTO.getNewEntity());
        assertEquals(scaleWithinTierActionApiDTO.getCurrentLocation(), scaleWithinTierActionApiDTO.getNewLocation());

        final ActionInfo actionInfoWithoutPrimaryProvider = getScaleWithinTierActionInfo(false);
        Explanation anoterScaleExplanation = Explanation.newBuilder()
                .setScale(ScaleExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()))
                .build();
        ActionSpec scaleWithinTierWithoutPrimaryProviderActionSpec = ActionSpec.newBuilder()
                .setExecutionStep(executionStepProgress)
                .setActionState(ActionDTO.ActionState.IN_PROGRESS)
                .setRecommendation(buildAction(actionInfoWithoutPrimaryProvider, anoterScaleExplanation))
                .build();
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        ActionApiDTO scaleWithinTierWithoutPrimaryProvider = mapper
                .mapActionSpecToActionApiDTO(scaleWithinTierWithoutPrimaryProviderActionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID, ActionDetailLevel.EXECUTION);
        assertNotNull(scaleWithinTierWithoutPrimaryProvider.getCurrentEntity());
        assertNull(scaleWithinTierWithoutPrimaryProvider.getCurrentEntity().getUuid());
        assertNull(scaleWithinTierWithoutPrimaryProvider.getNewEntity());
    }

    /**
     * Test the creation of the ActionExecutionAuditApiDTO, when status is in progress.
     */
    @Test
    public void testActionExecutionStatusInProgress() throws Exception {
        final int progress = 20;
        final String startTimeToString = DateTimeUtil.toString(MILLIS_2020_01_01_00_00_00);

        // In Progress
        final ActionDTO.ExecutionStep executionStepProgress = ActionDTO.ExecutionStep.newBuilder()
                .setStartTime(MILLIS_2020_01_01_00_00_00)
                .setProgressPercentage(progress)
                .build();
        final ActionInfo actionInfo = getHostMoveActionInfo();
        Explanation placement = Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setInitialPlacement(InitialPlacement.newBuilder())))
                .build();
        ActionSpec actionSpec = ActionSpec.newBuilder()
                .setExecutionStep(executionStepProgress)
                .setActionState(ActionDTO.ActionState.IN_PROGRESS)
                .setRecommendation(buildAction(actionInfo, placement))
                .build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        ActionExecutionAuditApiDTO executionDto = mapper
                .mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID, ActionDetailLevel.EXECUTION)
                .getExecutionStatus();
        assertEquals(com.vmturbo.api.enums.ActionState.IN_PROGRESS, executionDto.getState());
        assertEquals(new Integer(progress), executionDto.getProgress());
        assertEquals(startTimeToString, executionDto.getExecutionTime());
        assertNull(executionDto.getCompletionTime());

        // Pre In Progress
        final ActionDTO.ExecutionStep executionStepPre = ActionDTO.ExecutionStep.newBuilder()
                .setStartTime(MILLIS_2020_01_01_00_00_00)
                .setProgressPercentage(progress)
                .build();
        actionSpec = ActionSpec.newBuilder()
                .setExecutionStep(executionStepPre)
                .setActionState(ActionDTO.ActionState.PRE_IN_PROGRESS)
                .setRecommendation(buildAction(actionInfo, placement))
                .build();
        executionDto = mapper
                .mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID, ActionDetailLevel.EXECUTION)
                .getExecutionStatus();
        assertEquals(com.vmturbo.api.enums.ActionState.PRE_IN_PROGRESS, executionDto.getState());
        assertEquals(startTimeToString, executionDto.getExecutionTime());
        assertNull(executionDto.getCompletionTime());

        // Post In Progress
        final ActionDTO.ExecutionStep executionStepPost = ActionDTO.ExecutionStep.newBuilder()
                .setStartTime(MILLIS_2020_01_01_00_00_00)
                .setProgressPercentage(progress)
                .build();
        actionSpec = ActionSpec.newBuilder()
                .setExecutionStep(executionStepPost)
                .setActionState(ActionDTO.ActionState.POST_IN_PROGRESS)
                .setRecommendation(buildAction(actionInfo, placement))
                .build();
        executionDto = mapper
                .mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID, ActionDetailLevel.EXECUTION)
                .getExecutionStatus();
        assertEquals(com.vmturbo.api.enums.ActionState.POST_IN_PROGRESS, executionDto.getState());
        assertEquals(startTimeToString, executionDto.getExecutionTime());
        assertNull(executionDto.getCompletionTime());
    }

    /**
     * Test the creation of the ActionExecutionAuditApiDTO, when status is succeeded.
     */
    @Test
    public void testActionExecutionStatusSucceeded() throws Exception {
        final int progress = 100;
        final long completionTime = MILLIS_2020_01_01_00_00_00 + 10000;
        final String startTimeToString = DateTimeUtil.toString(MILLIS_2020_01_01_00_00_00);
        final String completionTimeToString = DateTimeUtil.toString(completionTime);
        final ActionInfo actionInfo = getHostMoveActionInfo();
        final ActionDTO.ExecutionStep executionStep = ActionDTO.ExecutionStep.newBuilder()
                .setStartTime(MILLIS_2020_01_01_00_00_00)
                .setProgressPercentage(progress)
                .setCompletionTime(completionTime)
                .build();
        Explanation placement = Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setInitialPlacement(InitialPlacement.newBuilder())))
                .build();
        final ActionSpec actionSpec = ActionSpec.newBuilder()
                .setExecutionStep(executionStep)
                .setActionState(ActionDTO.ActionState.SUCCEEDED)
                .setRecommendation(buildAction(actionInfo, placement))
                .build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        final ActionExecutionAuditApiDTO executionDto = mapper
                .mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID, ActionDetailLevel.EXECUTION)
                .getExecutionStatus();

        // Assert
        assertEquals(com.vmturbo.api.enums.ActionState.SUCCEEDED, executionDto.getState());
        assertNull(executionDto.getProgress());
        assertEquals(startTimeToString, executionDto.getExecutionTime());
        assertEquals(completionTimeToString, executionDto.getCompletionTime());
    }

    /**
     * Test the creation of the ActionExecutionAuditApiDTO, when status is failed.
     */
    @Test
    public void testActionExecutionStatusFailed() throws Exception {
        final int progress = 100;
        final long completionTime = MILLIS_2020_01_01_00_00_00 + 10000;
        final String startTimeToString = DateTimeUtil.toString(MILLIS_2020_01_01_00_00_00);
        final String completionTimeToString = DateTimeUtil.toString(completionTime);
        final List<String> messages = Lists.newArrayList("msg1", "msg2");
        final String lastMessage = Iterables.getLast(messages);
        final ActionInfo actionInfo = getHostMoveActionInfo();
        final ActionDTO.ExecutionStep executionStepPost = ActionDTO.ExecutionStep.newBuilder()
                .setStartTime(MILLIS_2020_01_01_00_00_00)
                .setProgressPercentage(progress)
                .setCompletionTime(completionTime)
                .addAllErrors(messages)
                .build();
        Explanation placement = Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setInitialPlacement(InitialPlacement.newBuilder())))
                .build();
        final ActionSpec actionSpec = ActionSpec.newBuilder()
                .setExecutionStep(executionStepPost)
                .setActionState(ActionDTO.ActionState.FAILED)
                .setRecommendation(buildAction(actionInfo, placement))
                .build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        final ActionExecutionAuditApiDTO executionDto = mapper
                .mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID, ActionDetailLevel.EXECUTION)
                .getExecutionStatus();

        // Assert
        assertEquals(com.vmturbo.api.enums.ActionState.FAILED, executionDto.getState());
        assertNull(executionDto.getProgress());
        assertEquals(startTimeToString, executionDto.getExecutionTime());
        assertEquals(completionTimeToString, executionDto.getCompletionTime());
        assertEquals(lastMessage, executionDto.getMessage());
    }

    /**
     * Test the creation of the ActionExecutionAuditApiDTO, when status is succeeded.
     */
    @Test
    public void testActionExecutionStatusQueued() throws Exception {
        final String startTimeToString = DateTimeUtil.toString(MILLIS_2020_01_01_00_00_00);
        final ActionInfo actionInfo = getHostMoveActionInfo();
        final ActionDTO.ExecutionStep executionStep = ActionDTO.ExecutionStep.newBuilder()
                .setStartTime(MILLIS_2020_01_01_00_00_00)
                .build();
        Explanation placement = Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setInitialPlacement(InitialPlacement.newBuilder())))
                .build();
        final ActionSpec actionSpec = ActionSpec.newBuilder()
                .setExecutionStep(executionStep)
                .setActionState(ActionDTO.ActionState.QUEUED)
                .setRecommendation(buildAction(actionInfo, placement))
                .build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);
        final ActionExecutionAuditApiDTO executionDto = mapper
                .mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID, ActionDetailLevel.EXECUTION)
                .getExecutionStatus();

        // Assert
        assertEquals(com.vmturbo.api.enums.ActionState.QUEUED, executionDto.getState());
        assertEquals(startTimeToString, executionDto.getExecutionTime());
        assertNull(executionDto.getProgress());
        assertNull(executionDto.getCompletionTime());
    }

    /**
     * Test valid actionCostType is mapped.
     */
    @Test
    public void testValidMapApiCostTypeToXL() {
        assertEquals(ActionDTO.ActionCostType.SAVINGS, ActionSpecMapper.mapApiCostTypeToXL(ActionCostType.SAVING));
        assertEquals(ActionDTO.ActionCostType.INVESTMENT, ActionSpecMapper.mapApiCostTypeToXL(ActionCostType.INVESTMENT));
        assertEquals(ActionDTO.ActionCostType.ACTION_COST_TYPE_NONE, ActionSpecMapper.mapApiCostTypeToXL(ActionCostType.ACTION_COST_TYPE_NONE));
    }

    /**
     * Test valid actionDisruptiveness is mapped.
     */
    @Test
    public void testValidMapApiDisruptivenessToXL() {
        assertEquals(ActionDTO.ActionDisruptiveness.DISRUPTIVE, ActionSpecMapper.mapApiDisruptivenessToXL(ActionDisruptiveness.DISRUPTIVE));
        assertEquals(ActionDTO.ActionDisruptiveness.NON_DISRUPTIVE, ActionSpecMapper.mapApiDisruptivenessToXL(ActionDisruptiveness.NON_DISRUPTIVE));
    }

    /**
     * Test valid actionDisruptiveness is mapped.
     */
    @Test
    public void testValidMapApiReversibilityToXL() {
        assertEquals(ActionDTO.ActionReversibility.REVERSIBLE, ActionSpecMapper.mapApiReversibilityToXL(ActionReversibility.REVERSIBLE));
        assertEquals(ActionDTO.ActionReversibility.IRREVERSIBLE, ActionSpecMapper.mapApiReversibilityToXL(ActionReversibility.IRREVERSIBLE));
    }

    /**
     * Test Exception thrown when invalid actionCostType is mapped.
     *
     * @throws IllegalArgumentException thrown if invalid actionCostType is mapped
     */
    @Test (expected = IllegalArgumentException.class)
    public void testInvalidMapApiCostTypeToXL() throws IllegalArgumentException {
        ActionSpecMapper.mapApiCostTypeToXL(ActionCostType.SUPER_SAVING);
    }

    /**
     * Verifies the conversion from XL ActionState to API ActionState.
     *
     * @throws Exception should not be thrown.
     */
    @Test
    public void testStateMappings() throws Exception {
        ActionInfo moveInfo = getHostMoveActionInfo();
        Explanation compliance = Explanation.newBuilder()
            .setMove(MoveExplanation.newBuilder()
                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                    .setCompliance(Compliance.newBuilder()
                        .addMissingCommodities(MEM)
                        .addMissingCommodities(CPU)
                        .build())
                    .build())
                .build())
            .build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet()))
                .thenReturn(dbReq);

        ActionSpec templateAction = buildActionSpec(moveInfo, compliance);
        assertActionState(templateAction, ActionDTO.ActionState.READY, ActionState.READY);
        assertActionState(templateAction, ActionDTO.ActionState.CLEARED, ActionState.CLEARED);
        assertActionState(templateAction, ActionDTO.ActionState.REJECTED, ActionState.REJECTED);
        assertActionState(templateAction, ActionDTO.ActionState.ACCEPTED, ActionState.ACCEPTED);
        assertActionState(templateAction, ActionDTO.ActionState.QUEUED, ActionState.QUEUED);
        assertActionState(templateAction, ActionDTO.ActionState.IN_PROGRESS, ActionState.IN_PROGRESS);
        assertActionState(templateAction, ActionDTO.ActionState.SUCCEEDED, ActionState.SUCCEEDED);
        assertActionState(templateAction, ActionDTO.ActionState.FAILED, ActionState.FAILED);
        assertActionState(templateAction, ActionDTO.ActionState.PRE_IN_PROGRESS, ActionState.IN_PROGRESS);
        assertActionState(templateAction, ActionDTO.ActionState.POST_IN_PROGRESS, ActionState.IN_PROGRESS);
        assertActionState(templateAction, ActionDTO.ActionState.FAILING, ActionState.FAILING);
    }

    /**
     * Test that when the {@link ActionApiInputDTO#getRelatedCloudServiceProviderIds()} is empty, then
     * {@link ActionSpecMapper#createActionFilter(ActionApiInputDTO, Optional, ApiId)} should return an
     * {@link ActionQueryFilter} instance with an empty
     * {@link ActionQueryFilter#getRelatedCloudServiceProviderIdsList()}.
     */
    @Test
    public void testCreateActionFilterNoRelatedCloudServiceProviders() {
        final ActionQueryFilter actionQueryFilter = mapper.createActionFilter(new ActionApiInputDTO(), Optional.empty(),
            null);
        Assert.assertTrue(actionQueryFilter.getRelatedCloudServiceProviderIdsList().isEmpty());
    }

    /**
     * Test that {@link ActionSpecMapper#createActionFilter(ActionApiInputDTO, Optional, ApiId)} returns an
     * {@link ActionQueryFilter} instance with {@link ActionQueryFilter#getRelatedCloudServiceProviderIdsList()}
     * containing only the numeric values from the provided {@link ActionApiInputDTO} instance.
     */
    @Test
    public void testCreateActionFilterNumericRelatedCloudServiceProviders() {
        final ActionApiInputDTO inputDTO = new ActionApiInputDTO();
        final String cspId = "12345";
        inputDTO.setRelatedCloudServiceProviderIds(Arrays.asList(cspId, "non-numeric-value"));
        final ActionQueryFilter actionQueryFilter = mapper.createActionFilter(inputDTO, Optional.empty(), null);
        Assert.assertEquals(Collections.singletonList(Long.parseLong(cspId)),
            actionQueryFilter.getRelatedCloudServiceProviderIdsList());
    }

    /**
     * Test the creation of the ExecutorInfoApiDTO for a schedule.
     */
    @Test
    public void testActionExecutorInfoForSchedule() throws Exception {
        ActionDTO.Action action = ActionDTO.Action.newBuilder().setId(106).setExecutorInfo(
                ExecutorInfo.newBuilder().setSchedule(ScheduleDetails.newBuilder().setScheduleId(
                        "1234").setScheduleName("test").build()).build()).setInfo(
                ActionInfo.newBuilder()
                        .setActivate(Activate.newBuilder().setTarget(ActionEntity.newBuilder()
                                .setId(123)
                                .setType(60)
                                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                                .build()).build())
                        .build()).setDeprecatedImportance(1).setExplanation(Explanation.newBuilder()
                .setActivate(ActivateExplanation.newBuilder().build())
                .build()).build();
        final ActionSpec actionSpec = ActionSpec.newBuilder().setActionState(
                ActionDTO.ActionState.SUCCEEDED).setRecommendation(action).build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet())).thenReturn(dbReq);
        final ActionExecutionAuditApiDTO executionDto = mapper.mapActionSpecToActionApiDTO(
                        actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID, ActionDetailLevel.EXECUTION)
                .getExecutionStatus();

        // Assert
        assertNotNull(executionDto.getExecutorInfo());
        assertEquals("test", executionDto.getExecutorInfo().getScheduleInfo().getScheduleName());
        assertEquals(ExecutorType.SCHEDULE, executionDto.getExecutorInfo().getType());
    }

    /**
     * Test the creation of the ExecutorInfoApiDTO for a USER.
     */
    @Test
    public void testActionExecutorInfoForUser() throws Exception {
        Action action = Action.newBuilder().setId(105).setExecutorInfo(
                ExecutorInfo.newBuilder().setUser(UserDetails.newBuilder()
                        .setUserId("123")
                        .setUserName("test")
                        .build()).build()).setInfo(ActionInfo.newBuilder()
                .setDeactivate(Deactivate.newBuilder().setTarget(ActionEntity.newBuilder()
                        .setId(123)
                        .setType(60)
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                        .build()).build())
                .build()).setDeprecatedImportance(1).setExplanation(Explanation.newBuilder()
                .setDeactivate(DeactivateExplanation.newBuilder().build())
                .build()).build();
        final ActionSpec actionSpec = ActionSpec.newBuilder().setActionState(
                ActionDTO.ActionState.SUCCEEDED).setRecommendation(action).build();
        final MultiEntityRequest dbReq = ApiTestUtils.mockMultiFullEntityReq(Lists.newArrayList());
        when(repositoryApi.entitiesRequest(Collections.emptySet())).thenReturn(dbReq);
        final ActionExecutionAuditApiDTO executionDto = mapper.mapActionSpecToActionApiDTO(
                        actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID, ActionDetailLevel.EXECUTION)
                .getExecutionStatus();

        // Assert
        assertNotNull(executionDto.getExecutorInfo());
        assertEquals("test", executionDto.getExecutorInfo().getUserInfo().getUserName());
        assertEquals(ExecutorType.USER, executionDto.getExecutorInfo().getType());
    }

    private void assertActionState(
            ActionSpec templateAction,
            ActionDTO.ActionState stateInActionOrchestrator,
            ActionState expectedState) throws Exception {
        final ActionSpec inputRequest = templateAction.toBuilder()
            .setActionState(stateInActionOrchestrator)
            .build();

        ActionApiDTO actionApiDTOFailed = mapper.mapActionSpecToActionApiDTO(
            inputRequest,
            REAL_TIME_TOPOLOGY_CONTEXT_ID);

        Assert.assertEquals(expectedState, actionApiDTOFailed.getActionState());
    }

    private ActionInfo getHostMoveActionInfo() {
        return getMoveActionInfo(ApiEntityType.PHYSICAL_MACHINE.apiStr(), true);
    }

    private ActionInfo getStorageMoveActionInfo() {
        return getMoveActionInfo(ApiEntityType.STORAGE.apiStr(), true);
    }

    private ActionInfo getMoveActionInfo(final String srcAndDestType, boolean hasSource) {
        return getMoveActionInfoWithResources(srcAndDestType, Collections.emptySet(), hasSource);
    }

    private ActionInfo getMoveActionInfoWithResources(final String srcAndDestType,
            Collection<Long> resources, boolean hasSource) {
        final ChangeProvider.Builder changeProviderBuilder = ChangeProvider.newBuilder();
        if (hasSource) {
            changeProviderBuilder.setSource(ApiUtilsTest.createActionEntity(1));
        }
        resources.forEach(resId -> changeProviderBuilder.addResource(ApiUtilsTest.createActionEntity(resId,
                ApiEntityType.VIRTUAL_VOLUME.typeNumber())));
        changeProviderBuilder.setDestination(ApiUtilsTest.createActionEntity(2))
                .build();

        Move move = Move.newBuilder()
                .setTarget(ApiUtilsTest.createActionEntity(3))
                .addChanges(changeProviderBuilder.build())
                .build();

        ActionInfo moveInfo = ActionInfo.newBuilder().setMove(move).build();

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(topologyEntityDTOList(Lists.newArrayList(
                new TestEntity(TARGET, 3L, EntityType.VIRTUAL_MACHINE_VALUE),
                new TestEntity(SOURCE, 1L, ApiEntityType.fromString(srcAndDestType).typeNumber()),
                new TestEntity(DESTINATION, 2L, ApiEntityType.fromString(srcAndDestType).typeNumber()))));
        when(repositoryApi.entitiesRequest(any()))
                .thenReturn(req);

        return moveInfo;
    }

    /**
     * Create information about action and involved entities. In this test case repository
     * contains information only about source and destination entities and target entity is
     * missed (simulates a situation when target entity is absent in topology).
     *
     * @param sourceEntityType entityType of source entity
     * @param targetEntityType entityType of target entity
     * @param targetEntityId id of target entity
     * @param targetEntityEnvType environment type of target entity
     * @return {@link ActionInfo} information about move action
     */
    private ActionInfo getMoveActionInfoForEntitiesAbsentInTopology(int sourceEntityType,
            int targetEntityType, int targetEntityId,
            EnvironmentTypeEnum.EnvironmentType targetEntityEnvType) {
        final ChangeProvider changeProvider = ChangeProvider.newBuilder()
                .setSource(ApiUtilsTest.createActionEntity(1))
                .setDestination(ApiUtilsTest.createActionEntity(2))
                .build();

        final Move move = Move.newBuilder()
                .setTarget(ActionEntity.newBuilder()
                        .setId(targetEntityId)
                        .setType(targetEntityType)
                        .setEnvironmentType(targetEntityEnvType)
                        .build())
                .addChanges(changeProvider)
                .build();

        final ActionInfo moveInfo = ActionInfo.newBuilder().setMove(move).build();

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(
                topologyEntityDTOList(Lists.newArrayList(new TestEntity(SOURCE, 1L, sourceEntityType),
                        new TestEntity(DESTINATION, 2L, sourceEntityType))));
        when(repositoryApi.entitiesRequest(any())).thenReturn(req);

        return moveInfo;
    }

    /**
     * Gets a new Scale {@link ActionInfo} object, with an optional primary provider set.
     *
     * @param withPrimaryProvider Whether the primary provider should be set
     * @return A new Scale {@link ActionInfo} object, with an optional primary provider set
     */
    private ActionInfo getScaleWithinTierActionInfo(boolean withPrimaryProvider) {
        long SOURCE_OID = 1L;
        Scale.Builder scaleBuilder = Scale.newBuilder()
                .setTarget(ApiUtilsTest.createActionEntity(3));
        if (withPrimaryProvider) {
            scaleBuilder.setPrimaryProvider(
                    ActionEntity.newBuilder()
                            .setType(EntityType.DATABASE_TIER_VALUE)
                            .setId(SOURCE_OID).build());
        }
        final ActionInfo scaleInfo = ActionInfo.newBuilder()
                .setScale(scaleBuilder)
                .build();
        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(topologyEntityDTOList(Lists.newArrayList(
                new TestEntity(TARGET, 3L, EntityType.DATABASE_VALUE)
        )));
        when(repositoryApi.entitiesRequest(any())).thenReturn(req);

        return scaleInfo;
    }

    private ActionInfo getScaleActionInfo() {
        final ActionInfo scaleInfo = ActionInfo.newBuilder()
            .setScale(Scale.newBuilder()
                .setTarget(ApiUtilsTest.createActionEntity(3))
                .addChanges(ChangeProvider.newBuilder()
                    .setSource(ApiUtilsTest.createActionEntity(1))
                    .setDestination(ApiUtilsTest.createActionEntity(2))))
            .build();

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(topologyEntityDTOList(Lists.newArrayList(
            new TestEntity(TARGET, 3L, EntityType.VIRTUAL_MACHINE_VALUE),
            new TestEntity(SOURCE, 1L, ApiEntityType.COMPUTE_TIER.typeNumber()),
            new TestEntity(DESTINATION, 2L, ApiEntityType.COMPUTE_TIER.typeNumber()))));
        when(repositoryApi.entitiesRequest(any())).thenReturn(req);

        return scaleInfo;
    }

    private ActionInfo getAllocateActionInfo() {

        final ActionInfo allocateInfo = ActionInfo.newBuilder()
                .setAllocate(Allocate.newBuilder()
                        .setTarget(ApiUtilsTest.createActionEntity(3))
                        .setWorkloadTier(ApiUtilsTest.createActionEntity(4)))
                .build();

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(topologyEntityDTOList(Lists.newArrayList(
                new TestEntity(TARGET, 3L, EntityType.VIRTUAL_MACHINE_VALUE),
                new TestEntity(SOURCE, 4L, ApiEntityType.COMPUTE_TIER.typeNumber()))));
        when(repositoryApi.entitiesRequest(any())).thenReturn(req);

        return allocateInfo;
    }

    private ApiPartialEntity topologyEntityDTO(@Nonnull final String displayName, long oid,
                                               int entityType) {
        ApiPartialEntity e = ApiPartialEntity.newBuilder()
            .setOid(oid)
            .setDisplayName(displayName)
            .setEntityType(entityType)
            // all entities to have same vendor id for testing
            .putDiscoveredTargetData(TARGET_ID, PerTargetEntityInformation.newBuilder()
                            .setVendorId(TARGET_VENDOR_ID).build())
            .build();

        final ServiceEntityApiDTO mappedE = new ServiceEntityApiDTO();
        mappedE.setDisplayName(displayName);
        mappedE.setUuid(Long.toString(oid));
        mappedE.setClassName(ApiEntityType.fromType(entityType).apiStr());
        final Map<Long, ServiceEntityApiDTO> map = new HashMap<>();
        map.put(oid, mappedE);
        when(serviceEntityMapper.toServiceEntityApiDTO(e)).thenReturn(mappedE);
        when(serviceEntityMapper.toServiceEntityApiDTOMap(any())).thenReturn(map);

        return e;
    }

    /**
     * A Test class to store information about entity, used for testing only.
     */
    final class TestEntity {
        public String displayName;
        public long oid;
        public int entityType;
        public List<BaseApiDTO> consumers;

        TestEntity(String displayName, long oid, int entityType) {
            this.displayName = displayName;
            this.oid = oid;
            this.entityType = entityType;
        }
    }

    private List<ApiPartialEntity> topologyEntityDTOList(List<TestEntity> testEntities) {
        Map<Long, ServiceEntityApiDTO> map = new HashMap<>();

        List<ApiPartialEntity> resp = testEntities.stream().map(testEntity -> {
            final ServiceEntityApiDTO mappedE = new ServiceEntityApiDTO();
            mappedE.setDisplayName(testEntity.displayName);
            mappedE.setUuid(Long.toString(testEntity.oid));
            mappedE.setClassName(ApiEntityType.fromType(testEntity.entityType).apiStr());
            mappedE.setConsumers(testEntity.consumers);
            map.put(testEntity.oid, mappedE);
            ApiPartialEntity e = ApiPartialEntity.newBuilder()
                    .setOid(testEntity.oid)
                    .setDisplayName(testEntity.displayName)
                    .setEntityType(testEntity.entityType)
                    // all entities to have same vendor id for testing
                    .putDiscoveredTargetData(TARGET_ID, PerTargetEntityInformation.newBuilder()
                            .setVendorId(TARGET_VENDOR_ID).build())
                    .build();
            when(serviceEntityMapper.toServiceEntityApiDTO(e)).thenReturn(mappedE);
            return e;
        }).collect(Collectors.toList());

        when(serviceEntityMapper.toServiceEntityApiDTOMap(any())).thenReturn(map);
        return resp;
    }

    private ActionSpec buildActionSpec(ActionInfo actionInfo, Explanation explanation) {
        return buildActionSpec(actionInfo, explanation, Optional.empty(), Optional.empty(), null);
    }

    private ActionSpec buildActionSpec(ActionInfo actionInfo, Explanation explanation,
                                       Optional<ActionDTO.ActionDecision> decision,
                                       Optional<ActionDTO.ExecutionStep> executionStep,
                                       ActionSpec.ActionSchedule schedule) {
        return buildActionSpec(actionInfo, explanation, decision, executionStep,
            schedule, null, null);
    }

    private ActionSpec buildActionSpec(ActionInfo actionInfo, Explanation explanation,
                                       Optional<ActionDTO.ActionDecision> decision,
                                       Optional<ActionDTO.ExecutionStep> executionStep,
                                       @Nullable ActionSpec.ActionSchedule schedule,
                                       @Nullable String externalName,
                                       @Nullable String externalUrl) {
        ActionSpec.Builder builder = ActionSpec.newBuilder()
            .setRecommendationTime(System.currentTimeMillis())
            .setRecommendation(buildAction(actionInfo, explanation))
            .setActionState(ActionDTO.ActionState.READY)
            .setActionMode(ActionMode.MANUAL)
            .setIsExecutable(true)
            .setExplanation(DEFAULT_EXPLANATION)
            .addPrerequisiteDescription(DEFAULT_PRE_REQUISITE_DESCRIPTION);

        executionStep.ifPresent((builder::setExecutionStep));
        decision.ifPresent(builder::setDecision);

        if (schedule != null) {
            builder.setActionSchedule(schedule);
        }

        if (externalName != null) {
            builder.setExternalActionName(externalName);
        }
        if (externalUrl != null) {
            builder.setExternalActionUrl(externalUrl);
        }

        return builder.build();
    }

    private ActionSpec buildActionSpecWithDisruptiveReversible(ActionInfo actionInfo, Explanation explanation,
            boolean disruptive, boolean reversible) {
        return ActionSpec.newBuilder()
                .setRecommendation(buildActionWithDisruptiveReversible(actionInfo, explanation, disruptive, reversible))
                .setExplanation(DEFAULT_EXPLANATION)
                .build();
    }

    private Action buildAction(ActionInfo actionInfo, Explanation explanation) {
        return Action.newBuilder()
                .setDeprecatedImportance(0)
                .setId(1234)
                .setInfo(actionInfo)
                .setExplanation(explanation)
                .build();
    }

    private Action buildActionWithDisruptiveReversible(ActionInfo actionInfo, Explanation explanation,
            boolean disruptive, boolean reversible) {
        return Action.newBuilder()
                .setDeprecatedImportance(0)
                .setId(1234)
                .setInfo(actionInfo)
                .setExplanation(explanation)
                .setDisruptive(disruptive)
                .setReversible(reversible)
                .build();
    }

    private static ReasonCommodity createReasonCommodity(int baseType, String key) {
        CommodityType.Builder ct = TopologyDTO.CommodityType.newBuilder()
                        .setType(baseType);
        if (key != null) {
            ct.setKey(key);
        }
        return ReasonCommodity.newBuilder().setCommodityType(ct.build()).build();
    }

    private GetMultiSupplyChainsResponse makeGetMultiSupplyChainResponse(long entityOid,
                                                                         long dataCenterOid) {
        return GetMultiSupplyChainsResponse.newBuilder().setSeedOid(entityOid).setSupplyChain(SupplyChain
            .newBuilder().addSupplyChainNodes(makeSupplyChainNode(dataCenterOid)))
            .build();
    }

    private SupplyChainNode makeSupplyChainNode(long oid) {
        return SupplyChainNode.newBuilder()
            .setEntityType(ApiEntityType.DATACENTER.typeNumber())
            .putMembersByState(EntityState.ACTIVE.ordinal(),
                MemberList.newBuilder().addMemberOids(oid).build())
            .build();
    }

    private ActionQueryFilter createFilter(final ActionApiInputDTO inputDto) {
        return mapper.createActionFilter(inputDto, Optional.empty(), null);
    }

    private PolicyApiDTO createPolicyApiDTO(long policyOid, String policyName) {
        PolicyApiDTO policyDto = new PolicyApiDTO();
        policyDto.setUuid(Long.toString(policyOid));
        policyDto.setDisplayName(policyName);
        policyDto.setName(policyName);
        return policyDto;
    }

    private BaseApiDTO createPolicyBaseApiDTO(long policyOid, String policyName, String className) {
        BaseApiDTO settingsPolicyDto = new BaseApiDTO();
        settingsPolicyDto.setUuid(Long.toString(policyOid));
        settingsPolicyDto.setDisplayName(policyName);
        settingsPolicyDto.setClassName(className);
        return settingsPolicyDto;
    }
}
