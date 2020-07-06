package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.api.component.external.api.mapper.ActionSpecMapper.mapXlActionStateToApi;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
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

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.hamcrest.collection.IsArrayContainingInAnyOrder;
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
import com.vmturbo.api.component.external.api.service.PoliciesService;
import com.vmturbo.api.component.external.api.util.ApiUtilsTest;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.action.ActionExecutionAuditApiDTO;
import com.vmturbo.api.dto.action.ActionScheduleApiDTO;
import com.vmturbo.api.dto.action.CloudResizeActionDetailsApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.enums.ActionCostType;
import com.vmturbo.api.enums.ActionDetailLevel;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.enums.EntityState;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.common.api.mappers.EnvironmentTypeMapper;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.Allocate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ActivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AllocateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Efficiency;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.InitialPlacement;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Performance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeactivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ScaleExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.CostSourceFilter;
import com.vmturbo.common.protobuf.cost.CostMoles;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.RIBuyContextFetchServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc;
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
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link ActionSpecMapper}.
 */
public class ActionSpecMapperTest {

    private static final int POLICY_ID = 10;
    private static final String POLICY_NAME = "policy";
    private static final String ENTITY_TO_RESIZE_NAME = "EntityToResize";
    private static final long REAL_TIME_TOPOLOGY_CONTEXT_ID = 777777L;
    private static final long CONTEXT_ID = 777L;

    private static final ReasonCommodity MEM =
                    createReasonCommodity(CommodityDTO.CommodityType.MEM_VALUE, "grah");
    private static final ReasonCommodity CPU =
                    createReasonCommodity(CommodityDTO.CommodityType.CPU_VALUE, "blah");
    private static final ReasonCommodity VMEM =
                    createReasonCommodity(CommodityDTO.CommodityType.VMEM_VALUE, "foo");
    private static final ReasonCommodity HEAP =
                    createReasonCommodity(CommodityDTO.CommodityType.HEAP_VALUE, "foo");

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

    private ActionSpecMapper mapper;

    private PoliciesService policiesService = mock(PoliciesService.class);

    private PolicyDTOMoles.PolicyServiceMole policyMole = spy(new PolicyServiceMole());

    private SupplyChainProtoMoles.SupplyChainServiceMole supplyChainMole =
        spy(new SupplyChainServiceMole());

    private CostMoles.ReservedInstanceUtilizationCoverageServiceMole reservedInstanceUtilizationCoverageServiceMole =
            spy(new CostMoles.ReservedInstanceUtilizationCoverageServiceMole());

    private CostMoles.CostServiceMole costServiceMole = spy(new CostMoles.CostServiceMole());

    private CostMoles.ReservedInstanceBoughtServiceMole reservedInstanceBoughtServiceMole =
            spy(new CostMoles.ReservedInstanceBoughtServiceMole());

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(policyMole, costServiceMole,
            reservedInstanceBoughtServiceMole, reservedInstanceUtilizationCoverageServiceMole);

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
        final List<PolicyResponse> policyResponses = ImmutableList.of(
            PolicyResponse.newBuilder().setPolicy(Policy.newBuilder()
                .setId(POLICY_ID)
                .setPolicyInfo(PolicyInfo.newBuilder()
                    .setName(POLICY_NAME)))
                .build());
        Mockito.when(policyMole.getPolicies(any())).thenReturn(policyResponses);
        PolicyServiceGrpc.PolicyServiceBlockingStub policyService =
                PolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());
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

        CostServiceGrpc.CostServiceBlockingStub costServiceBlockingStub =
                CostServiceGrpc.newBlockingStub(grpcServer.getChannel());
        ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub
                reservedInstanceUtilizationCoverageServiceBlockingStub =
                ReservedInstanceUtilizationCoverageServiceGrpc.newBlockingStub(grpcServer.getChannel());
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
                        policiesService);

        final BuyRiScopeHandler buyRiScopeHandler = mock(BuyRiScopeHandler.class);
        when(buyRiScopeHandler.extractActionTypes(emptyInputDto, scopeWithBuyRiActions))
                .thenReturn(buyRiActionTypes);
        when(buyRiScopeHandler.extractBuyRiEntities(scopeWithBuyRiActions))
                .thenReturn(buyRiOids);

        when(virtualVolumeAspectMapper.mapVirtualMachines(anySetOf(Long.class), anyLong())).thenReturn(Collections.emptyMap());
        when(virtualVolumeAspectMapper.mapUnattachedVirtualVolumes(anySetOf(Long.class), anyLong())).thenReturn(Optional.empty());

        mapper = new ActionSpecMapper(actionSpecMappingContextFactory,
            serviceEntityMapper, policiesService, reservedInstanceMapper, riBuyContextFetchServiceStub, costServiceBlockingStub,
                reservedInstanceUtilizationCoverageServiceBlockingStub, buyRiScopeHandler,
                REAL_TIME_TOPOLOGY_CONTEXT_ID);
    }

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

        // Validate that the importance value is 0
        assertEquals(0, actionApiDTO.getImportance(), 0.05);
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
                    .setEfficiency(Efficiency.newBuilder()))
                .build())
            .build();

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
            buildActionSpec(scaleInfo, explanation), REAL_TIME_TOPOLOGY_CONTEXT_ID);

        assertEquals(ActionType.SCALE, actionApiDTO.getActionType());
        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("3", actionApiDTO.getTarget().getUuid());
        assertEquals("default explanation", actionApiDTO.getRisk().getDescription());

        ActionApiDTO first = actionApiDTO.getCompoundActions().get(0);
        assertEquals(SOURCE, first.getCurrentEntity().getDisplayName());
        assertEquals("1", first.getCurrentValue());
        assertEquals(DESTINATION, first.getNewEntity().getDisplayName());
        assertEquals("2", first.getNewValue());

        // Validate that the importance value is 0
        assertEquals(0, actionApiDTO.getImportance(), 0.05);
    }

    /**
     * Test mapping of scheduled automated scale.
     * @throws Exception in case of mapping error.
     */
    @Test
    public void testMapScheduledScaleAutomated() throws Exception {
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
            Optional.empty(), schedule);

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

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
                buildActionSpec(allocateInfo, explanation), REAL_TIME_TOPOLOGY_CONTEXT_ID);

        assertEquals(ActionType.ALLOCATE, actionApiDTO.getActionType());
        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("3", actionApiDTO.getTarget().getUuid());
        assertEquals("4", actionApiDTO.getTemplate().getUuid());
        assertEquals("default explanation", actionApiDTO.getRisk().getDescription());
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
        Map<Long, Policy> policyMap = new HashMap();
        Policy policy = Policy.newBuilder().setId(policyOid).setPolicyInfo(PolicyInfo.newBuilder().setName(policyName)).build();
        policyMap.put(policyOid, policy);
        List<PolicyApiDTO> policyApiDto = new ArrayList<>();
        PolicyApiDTO policyDto = new PolicyApiDTO();
        policyApiDto.add(policyDto);
        policyDto.setUuid(String.valueOf(policyOid));
        policyDto.setName(policyName);
        policyDto.setDisplayName(policyName);
        ActionSpecMappingContext context = new ActionSpecMappingContext(entitiesMap,
            policyMap, Collections.emptyMap(), Collections.emptyMap(),
            Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
            Collections.emptyMap(), serviceEntityMapper, false, policyApiDto);

        final ReasonCommodity placement =
                        createReasonCommodity(CommodityDTO.CommodityType.SEGMENTATION_VALUE, String.valueOf(policyOid));
        ActionInfo info =
                    ActionInfo.newBuilder().setReconfigure(
                            Reconfigure.newBuilder()
                            .setTarget(ApiUtilsTest.createActionEntity(3))
                            .setSource(ApiUtilsTest.createActionEntity(1))
                            .build())
                    .build();
        Explanation reconfigure =
                    Explanation.newBuilder()
                            .setReconfigure(ReconfigureExplanation.newBuilder()
                                    .addReconfigureCommodity(placement).build())
                            .build();

        Action reconf = Action.newBuilder().setInfo(info).setId(2222L).setExplanation(reconfigure)
                .setDeprecatedImportance(1).build();
        ActionApiDTO output = new ActionApiDTO();
        mapper.populatePolicyForActionApiDto(reconf, output, context);

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
    }

    @Test
    public void testMapSourcelessReconfigure() throws Exception {
        final ReasonCommodity cpuAllocation =
                        createReasonCommodity(CommodityDTO.CommodityType.CPU_ALLOCATION_VALUE, null);

        ActionInfo moveInfo =
                    ActionInfo.newBuilder().setReconfigure(
                            Reconfigure.newBuilder()
                            .setTarget(ApiUtilsTest.createActionEntity(3))
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
    }

    @Test
    public void testMapProvisionRealtime() throws Exception {
        ActionInfo provisionInfo =
                ActionInfo.newBuilder()
                    .setProvision(Provision.newBuilder()
                        .setEntityToClone(ApiUtilsTest.createActionEntity(3))
                        .setProvisionedSeller(-1).build()).build();
        Explanation provision = Explanation.newBuilder().setProvision(ProvisionExplanation
                        .newBuilder().setProvisionBySupplyExplanation(ProvisionBySupplyExplanation
                                        .newBuilder().setMostExpensiveCommodityInfo(
                                                ReasonCommodity.newBuilder().setCommodityType(
                                                CommodityType.newBuilder().setType(21).build())
                                                        .build())).build()).build();

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
    }

    @Test
    public void testMapCloudResizeActionDetails() {
        final long targetId = 1;
        // mock on-demand cost response from cost service
        Cost.CloudCostStatRecord.StatRecord.StatValue statValueDto1 =
                Cost.CloudCostStatRecord.StatRecord.StatValue.newBuilder().setAvg(10f).setTotal(10f).build();
        Cost.CloudCostStatRecord.StatRecord.StatValue statValueDto2 =
                Cost.CloudCostStatRecord.StatRecord.StatValue.newBuilder().setAvg(20f).setTotal(20f).build();
        Cost.CloudCostStatRecord.StatRecord statRecord1 = Cost.CloudCostStatRecord.StatRecord.newBuilder().setValues(statValueDto1).build();
        Cost.CloudCostStatRecord.StatRecord statRecord2 = Cost.CloudCostStatRecord.StatRecord.newBuilder().setValues(statValueDto2).build();
        Cost.CloudCostStatRecord record1 = Cost.CloudCostStatRecord.newBuilder()
                .addStatRecords(statRecord1)
                .build();
        Cost.CloudCostStatRecord record2 = Cost.CloudCostStatRecord.newBuilder()
                .addStatRecords(statRecord2)
                .build();
        Cost.GetCloudCostStatsResponse serviceResult = Cost.GetCloudCostStatsResponse
                .newBuilder()
                .addCloudStatRecord(record1)
                .addCloudStatRecord(record2)
                .build();

        ArgumentCaptor<Cost.GetCloudCostStatsRequest> costParamCaptor =
            ArgumentCaptor.forClass(Cost.GetCloudCostStatsRequest.class);
        when(costServiceMole.getCloudCostStats(costParamCaptor.capture()))
            .thenReturn(Collections.singletonList(serviceResult));

        // test RI coverage before/after
        // mock responses
        Cost.EntityReservedInstanceCoverage mockCoverage = Cost.EntityReservedInstanceCoverage
                .newBuilder()
                .setEntityCouponCapacity(4)
                .putCouponsCoveredByRi(1, 1)
                .setEntityId(1)
                .build();
        Cost.GetEntityReservedInstanceCoverageResponse currentResponse = Cost.GetEntityReservedInstanceCoverageResponse
                .newBuilder().putCoverageByEntityId(1, mockCoverage).build();
        Cost.GetProjectedEntityReservedInstanceCoverageResponse projectedResponse =
                Cost.GetProjectedEntityReservedInstanceCoverageResponse.newBuilder()
                        .putCoverageByEntityId(1, mockCoverage).build();

        // mock call
        when(reservedInstanceUtilizationCoverageServiceMole
                .getEntityReservedInstanceCoverage(any()))
                .thenReturn(currentResponse);
        when(reservedInstanceUtilizationCoverageServiceMole
                .getProjectedEntityReservedInstanceCoverageStats(any()))
                .thenReturn(projectedResponse);

        // act
        CloudResizeActionDetailsApiDTO cloudResizeActionDetailsApiDTO = mapper.createCloudResizeActionDetailsDTO(targetId, null);

        // check
        assertNotNull(cloudResizeActionDetailsApiDTO);
        // on-demand cost
        assertEquals(cloudResizeActionDetailsApiDTO.getOnDemandCostBefore(), 10f,0);
        assertEquals(cloudResizeActionDetailsApiDTO.getOnDemandCostAfter(), 20f,0);
        // not implemented yet - set to $0 by default
        assertEquals(cloudResizeActionDetailsApiDTO.getOnDemandRateBefore(), 0f ,0);
        assertEquals(cloudResizeActionDetailsApiDTO.getOnDemandRateAfter(), 0f ,0);
        // ri coverage
        assertEquals(cloudResizeActionDetailsApiDTO.getRiCoverageBefore().getValue(), 1f, 0);
        assertEquals(cloudResizeActionDetailsApiDTO.getRiCoverageBefore().getCapacity().getAvg(), 4f , 0);
        assertEquals(cloudResizeActionDetailsApiDTO.getRiCoverageAfter().getValue(), 1f, 0);
        assertEquals(cloudResizeActionDetailsApiDTO.getRiCoverageAfter().getCapacity().getAvg(), 4f, 0);
        // check buy ri discount is excluded
        assertThat(costParamCaptor.getValue().getCloudCostStatsQueryList()
                        .iterator().next().getCostSourceFilter(),
                is(CostSourceFilter.newBuilder()
            .setExclusionFilter(true)
            .addCostSources(Cost.CostSource.BUY_RI_DISCOUNT)
            .build()));
    }

    /**
     * Test that the streaming response chunks from the cloud cost RPC are processed correctly.
     * Specifically, if the response chunks cumulatively contain exactly two records, the action
     * should have on-demand before and after costs drawn from these records. However, if a chunk
     * (or a series of chunks) contains more than two records, all remaining chunks are discarded
     * without being processed, and the action will not have these costs listed. (This also happens
     * if the total number of records from all chunks is less than two.)
     */
    @Test
    public void testCloudResizeOnDemandCosts() {
        final long targetId = 1;
        final Cost.CloudCostStatRecord record1 = Cost.CloudCostStatRecord.newBuilder()
            .addStatRecords(Cost.CloudCostStatRecord.StatRecord.newBuilder()
                .setValues(Cost.CloudCostStatRecord.StatRecord.StatValue.newBuilder()
                    .setAvg(10f)
                    .setTotal(10f)))
            .build();
        final Cost.CloudCostStatRecord record2 = Cost.CloudCostStatRecord.newBuilder()
            .addStatRecords(Cost.CloudCostStatRecord.StatRecord.newBuilder()
                .setValues(Cost.CloudCostStatRecord.StatRecord.StatValue.newBuilder()
                    .setAvg(20f)
                    .setTotal(20f)))
            .build();
        final Cost.CloudCostStatRecord record3 = Cost.CloudCostStatRecord.newBuilder()
            .addStatRecords(Cost.CloudCostStatRecord.StatRecord.newBuilder()
                .setValues(Cost.CloudCostStatRecord.StatRecord.StatValue.newBuilder()
                    .setAvg(30f)
                    .setTotal(30f)))
            .build();
        final Cost.GetCloudCostStatsResponse serviceResult = Cost.GetCloudCostStatsResponse
                .newBuilder()
                .addCloudStatRecord(record1)
                .addCloudStatRecord(record2)
                .addCloudStatRecord(record3)
                .build();
        Cost.GetCloudCostStatsResponse extraChunk1 =
            Cost.GetCloudCostStatsResponse.getDefaultInstance();
        Cost.GetCloudCostStatsResponse extraChunk2 =
            Cost.GetCloudCostStatsResponse.getDefaultInstance();

        when(costServiceMole.getCloudCostStats(any()))
            .thenReturn(Arrays.asList(serviceResult, extraChunk1, extraChunk2));

        CloudResizeActionDetailsApiDTO cloudResizeActionDetailsApiDTO =
            mapper.createCloudResizeActionDetailsDTO(targetId, null);

        assertEquals(0, cloudResizeActionDetailsApiDTO.getOnDemandCostBefore(), 0);
        assertEquals(0, cloudResizeActionDetailsApiDTO.getOnDemandCostAfter(), 0);
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
        assertEquals(CommodityDTO.CommodityType.CPU.name(),
                actionApiDTO.getRisk().getReasonCommodity());
        assertEquals(DC1_NAME, actionApiDTO.getCurrentLocation().getDisplayName());
        assertEquals(DC1_NAME, actionApiDTO.getNewLocation().getDisplayName());
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
        assertEquals(UICommodityType.VMEM.apiStr(),
                actionApiDTO.getRisk().getReasonCommodity());
        assertEquals("2097152.0", actionApiDTO.getCurrentValue());
        assertEquals("1048576.0", actionApiDTO.getResizeToValue());
        assertEquals(CommodityTypeUnits.VMEM.getUnits(), actionApiDTO.getValueUnits());
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
        assertEquals(UICommodityType.HEAP.apiStr(), actionApiDTO.getRisk().getReasonCommodity());
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
        assertEquals(UICommodityType.HEAP.apiStr(), actionApiDTO.getRisk().getReasonCommodity());
        // Check that the resize values are formatted in a consistent, API backwards-compatible way.
        assertEquals(String.format("%.1f", oldCapacity), actionApiDTO.getCurrentValue());
        assertEquals(String.format("%.1f", newCapacity), actionApiDTO.getResizeToValue());
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
        assertEquals(UICommodityType.MEM.apiStr(), actionApiDTO.getRisk().getReasonCommodity());
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
        Assert.assertThat(actionApiDTO.getRisk().getReasonCommodity().split(","),
            IsArrayContainingInAnyOrder.arrayContainingInAnyOrder(
                UICommodityType.CPU.apiStr(), UICommodityType.MEM.apiStr()));
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
        assertThat(actionApiDTO.getRisk().getReasonCommodity().split(","),
                IsArrayContainingInAnyOrder.arrayContainingInAnyOrder(
                    UICommodityType.CPU.apiStr(), UICommodityType.MEM.apiStr()));
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
        assertThat(actionApiDTO.getRisk().getReasonCommodity().split(","),
            IsArrayContainingInAnyOrder.arrayContainingInAnyOrder(
                UICommodityType.CPU.apiStr(), UICommodityType.MEM.apiStr()));
        assertEquals(DC1_NAME, actionApiDTO.getCurrentLocation().getDisplayName());
        assertNull(actionApiDTO.getNewLocation());
    }

    @Test
    public void testMapDelete() throws Exception {
        final long targetId = 1;
        final String fileName = "foobar";
        final String filePath = "/etc/local/" + fileName;
        final ActionInfo deleteInfo = ActionInfo.newBuilder()
            .setDelete(Delete.newBuilder().setTarget(ApiUtilsTest.createActionEntity(targetId))
                .setFilePath(filePath)
                .build())
            .build();
        Explanation delete = Explanation.newBuilder()
            .setDelete(DeleteExplanation.newBuilder().setSizeKb(2048l).build()).build();
        final String entityToDelete = "EntityToDelete";

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO(entityToDelete, targetId, EntityType.STORAGE_VALUE)));
        when(repositoryApi.entitiesRequest(Sets.newHashSet(targetId)))
            .thenReturn(req);

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
            buildActionSpec(deleteInfo, delete), CONTEXT_ID);
        // Verify that we set the context ID on the request.
        verify(req).contextId(CONTEXT_ID);

        assertEquals(entityToDelete, actionApiDTO.getTarget().getDisplayName());
        assertEquals(targetId, Long.parseLong(actionApiDTO.getTarget().getUuid()));
        assertEquals(ActionType.DELETE, actionApiDTO.getActionType());
        assertEquals(1, actionApiDTO.getVirtualDisks().size());
        assertEquals(filePath, actionApiDTO.getVirtualDisks().get(0).getDisplayName());
        assertEquals("2.0", actionApiDTO.getCurrentValue());
        assertEquals("MB", actionApiDTO.getValueUnits());
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
        assertThat(actionApiDTO.getRisk().getReasonCommodity().split(","),
                IsArrayContainingInAnyOrder.arrayContainingInAnyOrder(
                    UICommodityType.CPU.apiStr(), UICommodityType.MEM.apiStr()));
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

        Assert.assertEquals("target doesn't comply to " + POLICY_NAME,
                        dtos.get(0).getRisk().getDescription());
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
        Assert.assertEquals("target doesn't comply to " + POLICY_NAME,
            dtos1.get(0).getRisk().getDescription());

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
        Assert.assertEquals(Collections.singletonList(DEFAULT_PRE_REQUISITE_DESCRIPTION),
            dtos2.get(0).getPrerequisites());
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
            containsInAnyOrder(ActionSpecMapper.OPERATIONAL_ACTION_STATES));
    }

    // Similar fixes as in OM-24590: Do not show executed actions as pending,
    // when "inputDto" is null, we should automatically insert the operational action states.
    @Test
    public void testCreateActionFilterWithNoStateFilterAndNoInputDTO() {
        final ActionQueryFilter filter = createFilter(null);
        Assert.assertThat(filter.getStatesList(),
                containsInAnyOrder(ActionSpecMapper.OPERATIONAL_ACTION_STATES));
    }

    @Test
    public void testTranslateExplanation() {
        Map<Long, ApiPartialEntity> entitiesMap = new HashMap<>();
        ApiPartialEntity entity = topologyEntityDTO("Test Entity", 1L, EntityType.VIRTUAL_MACHINE_VALUE);
        entitiesMap.put(1L, entity);
        ActionSpecMappingContext context = new ActionSpecMappingContext(entitiesMap,
            Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
            Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
            Collections.emptyMap(), serviceEntityMapper, false, Collections.emptyList());
        context.getEntity(1L).get().setCostPrice(1.0f);

        String noTranslationNeeded = "Simple string";
        Assert.assertEquals("Simple string", ActionSpecMapper.translateExplanation(noTranslationNeeded, context));

        // test display name
        String translateName = ActionDTOUtil.TRANSLATION_PREFIX +"The entity name is "
                + ActionDTOUtil.createTranslationBlock(1, "displayName", "default");
        Assert.assertEquals("The entity name is Test Entity", ActionSpecMapper.translateExplanation(translateName, context));

        // test cost (numeric field)
        String translateCost = ActionDTOUtil.TRANSLATION_PREFIX +"The entity cost is "
                + ActionDTOUtil.createTranslationBlock(1, "costPrice", "I dunno, must be expensive");
        Assert.assertEquals("The entity cost is 1.0", ActionSpecMapper.translateExplanation(translateCost, context));

        // test fallback value
        String testFallback = ActionDTOUtil.TRANSLATION_PREFIX +"The entity madeup field is "
                + ActionDTOUtil.createTranslationBlock(1, "madeup", "fallback value");
        Assert.assertEquals("The entity madeup field is fallback value", ActionSpecMapper.translateExplanation(testFallback, context));
        // test blank fallback value
        String testBlankFallback = ActionDTOUtil.TRANSLATION_PREFIX +"The entity madeup field is "
                + ActionDTOUtil.createTranslationBlock(1, "madeup", "");
        Assert.assertEquals("The entity madeup field is ", ActionSpecMapper.translateExplanation(testBlankFallback, context));

        // test block at start of string
        String testStart = ActionDTOUtil.TRANSLATION_PREFIX
                + ActionDTOUtil.createTranslationBlock(1, "displayName", "default") +" and stuff";
        Assert.assertEquals("Test Entity and stuff", ActionSpecMapper.translateExplanation(testStart, context));

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
        final ActionExecutionAuditApiDTO executionDto = mapper
                .mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID, ActionDetailLevel.EXECUTION)
                .getExecutionStatus();

        // Assert
        assertNull(executionDto);
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
     * Test Exception thrown when invalid actionCostType is mapped.
     *
     * @throws IllegalArgumentException thrown if invalid actionCostType is mapped
     */
    @Test (expected = IllegalArgumentException.class)
    public void testInvalidMapApiCostTypeToXL() throws IllegalArgumentException {
        ActionSpecMapper.mapApiCostTypeToXL(ActionCostType.SUPER_SAVING);
    }

    private ActionInfo getHostMoveActionInfo() {
        return getMoveActionInfo(ApiEntityType.PHYSICAL_MACHINE.apiStr(), true);
    }

    private ActionInfo getStorageMoveActionInfo() {
        return getMoveActionInfo(ApiEntityType.STORAGE.apiStr(), true);
    }

    private ActionInfo getMoveActionInfo(final String srcAndDestType, boolean hasSource) {
        ChangeProvider changeProvider = hasSource ? ChangeProvider.newBuilder()
                .setSource(ApiUtilsTest.createActionEntity(1))
                .setDestination(ApiUtilsTest.createActionEntity(2))
                .build()
                : ChangeProvider.newBuilder()
                .setDestination(ApiUtilsTest.createActionEntity(2))
                .build();

        Move move = Move.newBuilder()
                .setTarget(ApiUtilsTest.createActionEntity(3))
                .addChanges(changeProvider)
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
        return builder.build();
    }
    private Action buildAction(ActionInfo actionInfo, Explanation explanation) {
        return Action.newBuilder()
            .setDeprecatedImportance(0)
            .setId(1234)
            .setInfo(actionInfo)
            .setExplanation(explanation)
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
            .setEntityType(ApiEntityType.DATACENTER.apiStr())
            .putMembersByState(EntityState.ACTIVE.ordinal(),
                MemberList.newBuilder().addMemberOids(oid).build())
            .build();
    }

    private ActionQueryFilter createFilter(final ActionApiInputDTO inputDto) {
        return mapper.createActionFilter(inputDto, Optional.empty(), null);
    }
}
