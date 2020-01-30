package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import javax.annotation.Nonnull;

import org.hamcrest.collection.IsArrayContainingInAnyOrder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMappingContextFactory.ActionSpecMappingContext;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.VirtualVolumeAspectMapper;
import com.vmturbo.api.component.external.api.util.ApiUtilsTest;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.action.CloudResizeActionDetailsApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.enums.EntityState;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
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
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
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

    private ActionSpecMapper mapper;

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

    private final ActionApiInputDTO emptyInputDto = new ActionApiInputDTO();
    private final ApiId scopeWithBuyRiActions = mock(ApiId.class);
    private final Set<ActionDTO.ActionType> buyRiActionTypes = ImmutableSet.of(
            ActionDTO.ActionType.BUY_RI);
    private final Set<Long> buyRiOids = ImmutableSet.of(999L);

    @Before
    public void setup() {
        RIBuyContextFetchServiceGrpc.RIBuyContextFetchServiceBlockingStub riBuyContextFetchServiceStub =
                RIBuyContextFetchServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final List<PolicyResponse> policyResponses = ImmutableList.of(
            PolicyResponse.newBuilder().setPolicy(Policy.newBuilder()
                .setId(POLICY_ID)
                .setPolicyInfo(PolicyInfo.newBuilder()
                    .setName(POLICY_NAME)))
                .build());
        Mockito.when(policyMole.getAllPolicies(any())).thenReturn(policyResponses);
        PolicyServiceGrpc.PolicyServiceBlockingStub policyService =
                PolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final List<GetMultiSupplyChainsResponse> supplyChainResponses = ImmutableList.of(
            makeGetMultiSupplyChainResponse(1L, DATACENTER1_ID),
            makeGetMultiSupplyChainResponse(2L, DATACENTER2_ID),
            makeGetMultiSupplyChainResponse(3L, DATACENTER2_ID));
        Mockito.when(supplyChainMole.getMultiSupplyChains(any())).thenReturn(supplyChainResponses);
        SupplyChainServiceBlockingStub supplyChainService =
                SupplyChainServiceGrpc.newBlockingStub(supplyChainGrpcServer.getChannel());

        VirtualVolumeAspectMapper volumeAspectMapper = mock(VirtualVolumeAspectMapper.class);

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
                new ActionSpecMappingContextFactory(policyService,
                        Executors.newCachedThreadPool(new ThreadFactoryBuilder().build()),
                        repositoryApi, mock(EntityAspectMapper.class), volumeAspectMapper,
                        REAL_TIME_TOPOLOGY_CONTEXT_ID, null, null, serviceEntityMapper,
                        supplyChainService);

        final BuyRiScopeHandler buyRiScopeHandler = mock(BuyRiScopeHandler.class);
        when(buyRiScopeHandler.extractActionTypes(emptyInputDto, scopeWithBuyRiActions))
                .thenReturn(buyRiActionTypes);
        when(buyRiScopeHandler.extractBuyRiEntities(scopeWithBuyRiActions))
                .thenReturn(buyRiOids);

        mapper = new ActionSpecMapper(actionSpecMappingContextFactory,
            serviceEntityMapper, reservedInstanceMapper, riBuyContextFetchServiceStub, costServiceBlockingStub,
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
        ActionInfo moveInfo = getMoveActionInfo(UIEntityType.STORAGE.apiStr(), false);
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
        ActionInfo moveInfo = getMoveActionInfo(UIEntityType.DISKARRAY.apiStr(), false);
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

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO(TARGET, 3L, EntityType.VIRTUAL_MACHINE_VALUE),
            topologyEntityDTO(SOURCE, 1L, EntityType.PHYSICAL_MACHINE_VALUE)));
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

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO(TARGET, 3L, EntityType.VIRTUAL_MACHINE_VALUE)));
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

        final long planId = 1 + REAL_TIME_TOPOLOGY_CONTEXT_ID;
        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
            buildActionSpec(provisionInfo, provision), planId);

        // Verify that we set the context ID on the request.
        verify(srcReq).contextId(planId);

        assertEquals("EntityToClone", actionApiDTO.getCurrentEntity().getDisplayName());
        assertEquals("3", actionApiDTO.getCurrentValue());

        assertEquals("EntityToClone", actionApiDTO.getTarget().getDisplayName());
        assertEquals("VirtualMachine", actionApiDTO.getTarget().getClassName());
        assertEquals("3", actionApiDTO.getTarget().getUuid());

        assertEquals("EntityToClone", actionApiDTO.getNewEntity().getDisplayName());
        assertEquals("VirtualMachine", actionApiDTO.getNewEntity().getClassName());
        assertEquals("-1", actionApiDTO.getNewEntity().getUuid());

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
        when(costServiceMole.getCloudCostStats(costParamCaptor.capture())).thenReturn(serviceResult);

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
        CloudResizeActionDetailsApiDTO cloudResizeActionDetailsApiDTO = mapper.createCloudResizeActionDetailsDTO(targetId);

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
                .setStartUtilization(0.2f)
                .setEndUtilization(0.4f).build())
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
                        .setStartUtilization(0.2f)
                        .setEndUtilization(0.4f).build())
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
        assertEquals(CommodityDTO.CommodityType.VMEM.name(),
                actionApiDTO.getRisk().getReasonCommodity());
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
                .setStartUtilization(0.2f)
                .setEndUtilization(0.4f).build())
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
        assertEquals(CommodityDTO.CommodityType.HEAP.name(),
            actionApiDTO.getRisk().getReasonCommodity());
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
                        .setStartUtilization(0.2f)
                        .setEndUtilization(0.4f).build())
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
        assertEquals(CommodityDTO.CommodityType.MEM.name(),
                actionApiDTO.getRisk().getReasonCommodity());
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
        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO("EntityToActivate", targetId, EntityType.VIRTUAL_MACHINE_VALUE)));
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
                    CommodityDTO.CommodityType.CPU.name(),
                    CommodityDTO.CommodityType.MEM.name()));
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

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO(entityToActivateName, targetId, EntityType.STORAGE_VALUE)));
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
                        CommodityDTO.CommodityType.CPU.name(), CommodityDTO.CommodityType.MEM.name()));
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

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO(entityToDeactivateName, targetId, EntityType.VIRTUAL_MACHINE_VALUE)));
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
                CommodityDTO.CommodityType.CPU.name(), CommodityDTO.CommodityType.MEM.name()));
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
                        CommodityDTO.CommodityType.CPU.name(), CommodityDTO.CommodityType.MEM.name()));
    }

    @Test
    public void testUpdateTime() throws Exception {

        // Arrange
        ActionInfo moveInfo = getHostMoveActionInfo();
        ActionDTO.ActionDecision decision = ActionDTO.ActionDecision.newBuilder()
                        .setDecisionTime(System.currentTimeMillis()).build();
        String expectedUpdateTime = DateTimeUtil.toString(decision.getDecisionTime());
        final ActionSpec actionSpec = buildActionSpec(moveInfo, Explanation.newBuilder().build(),
                        Optional.of(decision));

        // Act
        ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID);

        // Assert
        assertThat(actionApiDTO.getUpdateTime(), is(expectedUpdateTime));
    }

    @Test
    public void testMappingContinuesAfterError() throws Exception {
        final long badTarget = 3L;
        final long badSource = 1L;
        final long badDestination = 2L;
        final long goodTarget = 10L;

        final ActionInfo moveInfo = ActionInfo.newBuilder().setMove(Move.newBuilder()
                .setTarget(ApiUtilsTest.createActionEntity(badTarget))
                .addChanges(ChangeProvider.newBuilder()
                    .setSource(ApiUtilsTest.createActionEntity(badSource))
                    .setDestination(ApiUtilsTest.createActionEntity(badDestination))
                    .build())
                .build())
        .build();

        final ActionInfo resizeInfo = ActionInfo.newBuilder()
            .setResize(Resize.newBuilder()
                .setTarget(ApiUtilsTest.createActionEntity(goodTarget))
                .setOldCapacity(11)
                .setNewCapacity(12)
                .setCommodityType(CPU.getCommodityType()))
            .build();

        final ActionSpec moveSpec = buildActionSpec(moveInfo, Explanation.getDefaultInstance(), Optional.empty());
        final ActionSpec resizeSpec = buildActionSpec(resizeInfo, Explanation.getDefaultInstance(), Optional.empty());

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO("EntityToResize", goodTarget, EntityType.VIRTUAL_MACHINE_VALUE)));
        when(repositoryApi.entitiesRequest(ActionDTOUtil.getInvolvedEntityIds(Arrays.asList(
                moveSpec.getRecommendation(), resizeSpec.getRecommendation()))))
            .thenReturn(req);

        final List<ActionApiDTO> dtos = mapper.mapActionSpecsToActionApiDTOs(
                Arrays.asList(moveSpec, resizeSpec), CONTEXT_ID);
        // Verify that we set the context ID on the request.
        verify(req).contextId(CONTEXT_ID);

        assertEquals(1, dtos.size());
        assertEquals(ActionType.RESIZE, dtos.get(0).getActionType());
    }

    @Test
    public void testPlacementPolicyMove()
                    throws UnsupportedActionException, UnknownObjectException, ExecutionException,
                    InterruptedException {
        final ActionInfo moveInfo = ActionInfo.newBuilder().setMove(Move.newBuilder()
                        .setTarget(ApiUtilsTest.createActionEntity(1))
                        .addChanges(ChangeProvider.newBuilder()
                                        .setSource(ApiUtilsTest.createActionEntity(2))
                                        .setDestination(ApiUtilsTest.createActionEntity(3))
                                        .build())
                        .build())
                        .build();

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO("target", 1, EntityType.VIRTUAL_MACHINE_VALUE),
            topologyEntityDTO("source", 2, EntityType.VIRTUAL_MACHINE_VALUE),
            topologyEntityDTO("dest", 3, EntityType.VIRTUAL_MACHINE_VALUE)));
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
    public void testPlacementPolicyCompoundMove()
        throws UnsupportedActionException, UnknownObjectException, ExecutionException,
        InterruptedException {
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
        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO("target", 1, EntityType.VIRTUAL_MACHINE_VALUE),
            topologyEntityDTO("source", 2, EntityType.PHYSICAL_MACHINE_VALUE),
            topologyEntityDTO("dest", 3, EntityType.PHYSICAL_MACHINE_VALUE),
            topologyEntityDTO("stSource", 4, EntityType.STORAGE_VALUE),
            topologyEntityDTO("stDest", 5, EntityType.STORAGE_VALUE)));
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
    public void testMapReadyRecommendModeExecutable() throws InterruptedException, UnknownObjectException,
                                                             UnsupportedActionException, ExecutionException {
        final ActionSpec actionSpec = buildActionSpec(getHostMoveActionInfo(), Explanation.getDefaultInstance()).toBuilder()
            // The action is in READY state, and in RECOMMEND mode.
            .setActionState(ActionState.READY)
            .setActionMode(ActionMode.RECOMMEND)
            .build();
        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertThat(actionApiDTO.getActionState(), is(com.vmturbo.api.enums.ActionState.READY));
    }

    @Test
    public void testMapReadyRecommendModeNotExecutable() throws InterruptedException, UnknownObjectException,
                                                                UnsupportedActionException, ExecutionException {
        final ActionSpec actionSpec = buildActionSpec(getHostMoveActionInfo(), Explanation.getDefaultInstance()).toBuilder()
            .setActionState(ActionState.READY)
            .setActionMode(ActionMode.RECOMMEND)
            .setIsExecutable(false)
            .build();
        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertThat(actionApiDTO.getActionState(), is(com.vmturbo.api.enums.ActionState.READY));
    }

    @Test
    public void testMapReadyNotRecommendModeExecutable() throws InterruptedException, UnknownObjectException,
                                                                UnsupportedActionException, ExecutionException {
        final ActionSpec actionSpec = buildActionSpec(getHostMoveActionInfo(), Explanation.getDefaultInstance()).toBuilder()
            // The action is in READY state, and in RECOMMEND mode.
            .setActionState(ActionState.READY)
            .setActionMode(ActionMode.MANUAL)
            .build();
        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertThat(actionApiDTO.getActionState(), is(com.vmturbo.api.enums.ActionState.READY));
    }

    @Test
    public void testMapReadyNotRecommendModeNotExecutable() throws InterruptedException, UnknownObjectException,
                                                                   UnsupportedActionException, ExecutionException {
        final ActionSpec actionSpec = buildActionSpec(getHostMoveActionInfo(), Explanation.getDefaultInstance()).toBuilder()
            .setActionState(ActionState.READY)
            .setActionMode(ActionMode.MANUAL)
            .setIsExecutable(false)
            .build();
        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(actionSpec, REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertThat(actionApiDTO.getActionState(), is(com.vmturbo.api.enums.ActionState.READY));
    }

    @Test
    public void testMapNotReadyRecommendModeExecutable() throws InterruptedException, UnknownObjectException,
        UnsupportedActionException, ExecutionException {
        final ActionSpec actionSpec = buildActionSpec(getHostMoveActionInfo(), Explanation.getDefaultInstance()).toBuilder()
            .setActionState(ActionState.QUEUED)
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
     * Test that the related entity types from {@link ActionApiInputDTO} makes its way into
     * the mapped {@link ActionQueryFilter}.
     */
    @Test
    public void testCreateActionFilterWithInvolvedEntityTypes() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        inputDto.setRelatedEntityTypes(Arrays.asList(UIEntityType.VIRTUAL_MACHINE.apiStr(),
            UIEntityType.PHYSICAL_MACHINE.apiStr()));

        final ActionQueryFilter filter = createFilter(inputDto);

        assertThat(filter.getEntityTypeList(),
            containsInAnyOrder(UIEntityType.VIRTUAL_MACHINE.typeNumber(),
                UIEntityType.PHYSICAL_MACHINE.typeNumber()));
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
            Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
            Collections.emptyMap(), serviceEntityMapper, false);
        context.getOptionalEntity(1L).get().setCostPrice(1.0f);

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
        Optional<ActionDTO.ActionState> state = mapper.mapApiStateToXl(com.vmturbo.api.enums.ActionState.CLEARED);
        assertThat(state.get(), is(ActionState.CLEARED));
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
            .setActionState(ActionState.POST_IN_PROGRESS);
        ActionSpec actionSpec = builder.build();
        com.vmturbo.api.enums.ActionState actionState = mapper.mapXlActionStateToApi(actionSpec.getActionState());
        assertThat(actionState, is(com.vmturbo.api.enums.ActionState.IN_PROGRESS));
    }

    @Test
    public void testMapPreInProgress() {
        ActionSpec.Builder builder = ActionSpec.newBuilder()
            .setActionState(ActionState.PRE_IN_PROGRESS);
        ActionSpec actionSpec = builder.build();
        com.vmturbo.api.enums.ActionState actionState = mapper.mapXlActionStateToApi(actionSpec.getActionState());
        assertThat(actionState, is(com.vmturbo.api.enums.ActionState.IN_PROGRESS));
    }

    @Test
    public void testGetUserName(){
        final String sampleUserUuid = "administrator(22222222222)";
        assertEquals("administrator", mapper.getUserName(sampleUserUuid));
        assertEquals(AuditLogUtils.SYSTEM, mapper.getUserName(AuditLogUtils.SYSTEM));
    }

    private ActionInfo getHostMoveActionInfo() {
        return getMoveActionInfo(UIEntityType.PHYSICAL_MACHINE.apiStr(), true);
    }

    private ActionInfo getStorageMoveActionInfo() {
        return getMoveActionInfo(UIEntityType.STORAGE.apiStr(), true);
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

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO(TARGET, 3L, EntityType.VIRTUAL_MACHINE_VALUE),
            topologyEntityDTO(SOURCE, 1L, UIEntityType.fromString(srcAndDestType).typeNumber()),
            topologyEntityDTO(DESTINATION, 2L, UIEntityType.fromString(srcAndDestType).typeNumber())));
        when(repositoryApi.entitiesRequest(any()))
            .thenReturn(req);

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

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
            topologyEntityDTO(TARGET, 3L, EntityType.VIRTUAL_MACHINE_VALUE),
            topologyEntityDTO(SOURCE, 1L, UIEntityType.COMPUTE_TIER.typeNumber()),
            topologyEntityDTO(DESTINATION, 2L, UIEntityType.COMPUTE_TIER.typeNumber())));
        when(repositoryApi.entitiesRequest(any())).thenReturn(req);

        return scaleInfo;
    }

    private ActionInfo getAllocateActionInfo() {
        final ActionInfo allocateInfo = ActionInfo.newBuilder()
                .setAllocate(Allocate.newBuilder()
                        .setTarget(ApiUtilsTest.createActionEntity(3))
                        .setWorkloadTier(ApiUtilsTest.createActionEntity(4)))
                .build();

        final MultiEntityRequest req = ApiTestUtils.mockMultiEntityReq(Lists.newArrayList(
                topologyEntityDTO(TARGET, 3L, EntityType.VIRTUAL_MACHINE_VALUE),
                topologyEntityDTO(SOURCE, 4L, UIEntityType.COMPUTE_TIER.typeNumber())));
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
        mappedE.setClassName(UIEntityType.fromType(entityType).apiStr());
        when(serviceEntityMapper.toServiceEntityApiDTO(e)).thenReturn(mappedE);

        return e;
    }

    private ActionSpec buildActionSpec(ActionInfo actionInfo, Explanation explanation) {
        return buildActionSpec(actionInfo, explanation, Optional.empty());
    }

    private ActionSpec buildActionSpec(ActionInfo actionInfo, Explanation explanation,
                    Optional<ActionDTO.ActionDecision> decision) {
        ActionSpec.Builder builder = ActionSpec.newBuilder()
            .setRecommendationTime(System.currentTimeMillis())
            .setRecommendation(buildAction(actionInfo, explanation))
            .setActionState(ActionState.READY)
            .setActionMode(ActionMode.MANUAL)
            .setIsExecutable(true)
            .setExplanation(DEFAULT_EXPLANATION)
            .addPrerequisiteDescription(DEFAULT_PRE_REQUISITE_DESCRIPTION);

        decision.ifPresent(builder::setDecision);
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
            .setEntityType(UIEntityType.DATACENTER.apiStr())
            .putMembersByState(EntityState.ACTIVE.ordinal(),
                MemberList.newBuilder().addMemberOids(oid).build())
            .build();
    }

    private ActionQueryFilter createFilter(final ActionApiInputDTO inputDto) {
        return mapper.createActionFilter(inputDto, Optional.empty(), null);
    }
}
