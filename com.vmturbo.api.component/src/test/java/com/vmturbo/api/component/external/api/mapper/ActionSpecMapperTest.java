package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;

import java.io.IOException;
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
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMappingContextFactory.ActionSpecMappingContext;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.component.external.api.mapper.aspect.CloudAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.VirtualMachineAspectMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.VirtualVolumeAspectMapper;
import com.vmturbo.api.component.external.api.util.ApiUtilsTest;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ActivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.InitialPlacement;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Performance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeactivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.search.Search.SearchPlanTopologyEntityDTOsResponse;
import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsResponse;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link ActionSpecMapper}.
 */
public class ActionSpecMapperTest {

    public static final int POLICY_ID = 10;
    public static final String POLICY_NAME = "policy";
    private static final String ENTITY_TO_RESIZE_NAME = "EntityToResize";
    private static final long REAL_TIME_TOPOLOGY_CONTEXT_ID = 777777L;
    private ActionSpecMapper mapper;

    private ActionSpecMappingContextFactory actionSpecMappingContextFactory;

    private CloudAspectMapper cloudAspectMapper;

    private VirtualMachineAspectMapper vmAspectMapper;

    private VirtualVolumeAspectMapper volumeAspectMapper;

    private RepositoryApi repositoryApi;

    private PolicyDTOMoles.PolicyServiceMole policyMole;

    private GrpcTestServer grpcServer;

    private PolicyServiceGrpc.PolicyServiceBlockingStub policyService;

    private SearchServiceMole searchMole;

    private final long contextId = 777L;

    private CommodityType commodityCpu;
    private CommodityType commodityMem;
    private CommodityType commodityVMem;

    private static final String START = "Start";
    private static final String TARGET = "Target";
    private static final String SOURCE = "Source";
    private static final String DESTINATION = "Destination";
    private static final String DEFAULT_EXPLANATION = "default explanation";

    @Before
    public void setup() throws IOException {
        policyMole = Mockito.spy(PolicyDTOMoles.PolicyServiceMole.class);
        final List<PolicyResponse> policyResponses = ImmutableList.of(
            PolicyResponse.newBuilder().setPolicy(Policy.newBuilder()
                .setId(POLICY_ID)
                .setPolicyInfo(PolicyInfo.newBuilder()
                    .setName(POLICY_NAME)))
                .build());
        Mockito.when(policyMole.getAllPolicies(Mockito.any())).thenReturn(policyResponses);
        grpcServer = GrpcTestServer.newServer(policyMole);
        grpcServer.start();
        policyService = PolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());
        repositoryApi = Mockito.mock(RepositoryApi.class);

        searchMole = Mockito.spy(new SearchServiceMole());
        GrpcTestServer searchGrpcServer = GrpcTestServer.newServer(searchMole);
        searchGrpcServer.start();
        SearchServiceBlockingStub searchServiceBlockingStub = SearchServiceGrpc.newBlockingStub(
            searchGrpcServer.getChannel());

        cloudAspectMapper = Mockito.mock(CloudAspectMapper.class);
        vmAspectMapper = Mockito.mock(VirtualMachineAspectMapper.class);
        volumeAspectMapper = Mockito.mock(VirtualVolumeAspectMapper.class);

        actionSpecMappingContextFactory = new ActionSpecMappingContextFactory(policyService,
            Executors.newCachedThreadPool(new ThreadFactoryBuilder().build()),
            searchServiceBlockingStub, cloudAspectMapper, vmAspectMapper, volumeAspectMapper,
            REAL_TIME_TOPOLOGY_CONTEXT_ID);
        mapper = new ActionSpecMapper(actionSpecMappingContextFactory, REAL_TIME_TOPOLOGY_CONTEXT_ID);
        commodityCpu = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.CPU_VALUE)
            .setKey("blah")
            .build();
        commodityMem = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.MEM_VALUE)
            .setKey("grah")
            .build();
        commodityVMem = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.VMEM_VALUE)
            .setKey("foo")
            .build();
    }

    @Test
    public void testMapMove() throws Exception {
        ActionInfo moveInfo = getHostMoveActionInfo();
        Explanation compliance = Explanation.newBuilder()
            .setMove(MoveExplanation.newBuilder()
                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                    .setCompliance(Compliance.newBuilder()
                        .addMissingCommodities(commodityMem)
                        .addMissingCommodities(commodityCpu)
                        .build())
                    .build())
                .build())
            .build();
        ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, compliance), contextId);
        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("3", actionApiDTO.getTarget().getUuid());

        ActionApiDTO first = actionApiDTO.getCompoundActions().get(0);
        assertEquals(SOURCE, first.getCurrentEntity().getDisplayName());
        assertEquals("1", first.getCurrentValue());

        assertEquals(DESTINATION, first.getNewEntity().getDisplayName());
        assertEquals("2", first.getNewValue());

        assertEquals(ActionType.MOVE, actionApiDTO.getActionType());
        assertEquals("default explanation", actionApiDTO.getRisk().getDescription());

        assertTrue(actionApiDTO.getDetails().startsWith("Move"));
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
            mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, placement), contextId);
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
        assertEquals(null, actionApiDTO.getCurrentValue());
        assertEquals("default explanation", actionApiDTO.getRisk().getDescription());

        assertTrue(actionApiDTO.getDetails().startsWith(START));
    }

    @Test
    public void testMapStorageMove() throws Exception {
        ActionInfo moveInfo = getStorageMoveActionInfo();
        Explanation compliance = Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setCompliance(Compliance.newBuilder()
                                .addMissingCommodities(commodityMem)
                                .addMissingCommodities(commodityCpu).build())
                        .build())
                    .build())
                .build();
        ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, compliance), contextId);
        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("3", actionApiDTO.getTarget().getUuid());

        assertEquals(1, actionApiDTO.getCompoundActions().size());
        ActionApiDTO first = actionApiDTO.getCompoundActions().get(0);
        assertEquals(SOURCE, first.getCurrentEntity().getDisplayName());
        assertEquals("1", first.getCurrentValue());

        assertEquals(DESTINATION, first.getNewEntity().getDisplayName());
        assertEquals("2", first.getNewValue());

        assertEquals(ActionType.CHANGE, actionApiDTO.getActionType());
        assertEquals("default explanation", actionApiDTO.getRisk().getDescription());
    }

    /**
     * If move action doesn't have the source entity/id, it's ADD_PROVIDER for Storage.
     *
     * @throws Exception
     */
    @Test
    public void testMapStorageMoveWithoutSourceId() throws Exception {
        ActionInfo moveInfo = getMoveActionInfo(UIEntityType.STORAGE.getValue(), false);
        Explanation compliance = Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setCompliance(Compliance.newBuilder()
                                        .addMissingCommodities(commodityMem)
                                        .addMissingCommodities(commodityCpu).build())
                                .build())
                        .build())
                .build();
        ActionApiDTO actionApiDTO =
                mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, compliance), contextId);

        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("3", actionApiDTO.getTarget().getUuid());

        assertEquals(1, actionApiDTO.getCompoundActions().size());
        ActionApiDTO first = actionApiDTO.getCompoundActions().get(0);
        assertEquals(DESTINATION, first.getNewEntity().getDisplayName());
        assertEquals("2", first.getNewValue());

        assertEquals(ActionType.ADD_PROVIDER, actionApiDTO.getActionType());
        assertEquals("default explanation", actionApiDTO.getRisk().getDescription());
    }

    /**
     * If move action doesn't have the source entity/id, it's START except Storage;
     *
     * @throws Exception
     */
    @Test
    public void testMapDiskArrayMoveWithoutSourceId() throws Exception {
        ActionInfo moveInfo = getMoveActionInfo(UIEntityType.DISKARRAY.getValue(), false);
        Explanation compliance = Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setCompliance(Compliance.newBuilder()
                                        .addMissingCommodities(commodityMem)
                                        .addMissingCommodities(commodityCpu).build())
                                .build())
                        .build())
                .build();
        ActionApiDTO actionApiDTO =
                mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, compliance), contextId);

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
        final CommodityType cpuAllocation = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.CPU_ALLOCATION_VALUE)
            .build();
        final CommodityType network = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.NETWORK_VALUE)
                .setKey("TestNetworkName1")
                .build();

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
        Mockito.when(searchMole.searchPlanTopologyEntityDTOs(any()))
            .thenReturn(SearchPlanTopologyEntityDTOsResponse.newBuilder()
                .addAllTopologyEntityDtos(Lists.newArrayList(
                    topologyEntityDTO(TARGET, 3L, EntityType.VIRTUAL_MACHINE_VALUE),
                    topologyEntityDTO(SOURCE, 1L, EntityType.PHYSICAL_MACHINE_VALUE)
                )).build());

        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, reconfigure), contextId);
        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("3", actionApiDTO.getTarget().getUuid());
        assertEquals("VirtualMachine", actionApiDTO.getTarget().getClassName());

        assertEquals(SOURCE, actionApiDTO.getCurrentEntity().getDisplayName());
        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("1", actionApiDTO.getCurrentValue());

        assertEquals( ActionType.RECONFIGURE, actionApiDTO.getActionType());
        assertEquals(
            "Reconfigure Virtual Machine Target which requires Cpu Allocation, Network TestNetworkName1 but " +
                    "is hosted by Physical Machine Source which does not provide Cpu Allocation, Network " +
                    "TestNetworkName1",
            actionApiDTO.getDetails());
    }

    @Test
    public void testMapSourcelessReconfigure() throws Exception {
        final CommodityType cpuAllocation = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.CPU_ALLOCATION_VALUE)
            .build();

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
        Mockito.when(searchMole.searchPlanTopologyEntityDTOs(any()))
            .thenReturn(SearchPlanTopologyEntityDTOsResponse.newBuilder()
                .addAllTopologyEntityDtos(Lists.newArrayList(
                    topologyEntityDTO(TARGET, 3L, EntityType.VIRTUAL_MACHINE_VALUE)
                )).build());
        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, reconfigure), contextId);
        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("3", actionApiDTO.getTarget().getUuid());
        assertEquals("VirtualMachine", actionApiDTO.getTarget().getClassName());

        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());

        assertEquals( ActionType.RECONFIGURE, actionApiDTO.getActionType());
        assertEquals(
            "Reconfigure Virtual Machine Target as it is unplaced",
            actionApiDTO.getDetails());
    }

    @Test
    public void testMapProvision() throws Exception {
        ActionInfo provisionInfo =
                ActionInfo.newBuilder()
                    .setProvision(Provision.newBuilder()
                        .setEntityToClone(ApiUtilsTest.createActionEntity(3))
                        .setProvisionedSeller(-1).build()).build();
        Explanation provision = Explanation.newBuilder().setProvision(ProvisionExplanation
                        .newBuilder().setProvisionBySupplyExplanation(ProvisionBySupplyExplanation
                                        .newBuilder().setMostExpensiveCommodity(21).build())
                        .build()).build();
        Mockito.when(searchMole.searchPlanTopologyEntityDTOs(any()))
            .thenReturn(SearchPlanTopologyEntityDTOsResponse.newBuilder()
                .addAllTopologyEntityDtos(Lists.newArrayList(
                    topologyEntityDTO("EntityToClone", 3L, EntityType.VIRTUAL_MACHINE_VALUE),
                    topologyEntityDTO("New Entity", -1L, EntityType.VIRTUAL_MACHINE_VALUE)
                )).build());

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
                buildActionSpec(provisionInfo, provision), contextId);
        assertEquals("EntityToClone", actionApiDTO.getCurrentEntity().getDisplayName());
        assertEquals("3", actionApiDTO.getCurrentValue());

        assertEquals("EntityToClone", actionApiDTO.getTarget().getDisplayName());
        assertEquals("VirtualMachine", actionApiDTO.getTarget().getClassName());
        assertEquals("3", actionApiDTO.getTarget().getUuid());

        assertEquals("EntityToClone", actionApiDTO.getNewEntity().getDisplayName());
        assertEquals("VirtualMachine", actionApiDTO.getNewEntity().getClassName());
        assertEquals("3", actionApiDTO.getNewEntity().getUuid());

        assertEquals(ActionType.PROVISION, actionApiDTO.getActionType());
        assertThat(actionApiDTO.getDetails(), containsString("Provision Virtual Machine EntityToClone"));
    }

    @Test
    public void testMapResize() throws Exception {
        final long targetId = 1;
        final ActionInfo resizeInfo = ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                    .setTarget(ApiUtilsTest.createActionEntity(targetId))
                    .setOldCapacity(9)
                    .setNewCapacity(10)
                    .setCommodityType(commodityCpu))
            .build();

        Explanation resize = Explanation.newBuilder()
            .setResize(ResizeExplanation.newBuilder()
                .setStartUtilization(0.2f)
                .setEndUtilization(0.4f).build())
            .build();

        Mockito.when(searchMole.searchPlanTopologyEntityDTOs(any()))
            .thenReturn(SearchPlanTopologyEntityDTOsResponse.newBuilder()
                .addAllTopologyEntityDtos(Lists.newArrayList(
                    topologyEntityDTO(ENTITY_TO_RESIZE_NAME, targetId, EntityType.VIRTUAL_MACHINE_VALUE)
                )).build());

        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(buildActionSpec(resizeInfo, resize), contextId);
        assertEquals(ENTITY_TO_RESIZE_NAME, actionApiDTO.getTarget().getDisplayName());
        assertEquals(targetId, Long.parseLong(actionApiDTO.getTarget().getUuid()));
        assertEquals(ActionType.RESIZE, actionApiDTO.getActionType());
        assertEquals(CommodityDTO.CommodityType.CPU.name(),
                actionApiDTO.getRisk().getReasonCommodity());
    }

    @Test
    public void testResizeVMemDetail() throws Exception {
        final long targetId = 1;
        final ActionInfo resizeInfo = ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                        .setTarget(ApiUtilsTest.createActionEntity(targetId))
                        .setOldCapacity(1024 * 1024 * 2)
                        .setNewCapacity(1024 * 1024 * 1)
                        .setCommodityType(commodityVMem))
                .build();
        final String expectedDetailCapacityy = "from 2 GB to 1 GB";
        Explanation resize = Explanation.newBuilder()
                .setResize(ResizeExplanation.newBuilder()
                        .setStartUtilization(0.2f)
                        .setEndUtilization(0.4f).build())
                .build();
        Mockito.when(searchMole.searchPlanTopologyEntityDTOs(any()))
            .thenReturn(SearchPlanTopologyEntityDTOsResponse.newBuilder()
                .addAllTopologyEntityDtos(Lists.newArrayList(
                    topologyEntityDTO(ENTITY_TO_RESIZE_NAME, targetId, EntityType.VIRTUAL_MACHINE_VALUE)
                )).build());
        final ActionApiDTO actionApiDTO =
                mapper.mapActionSpecToActionApiDTO(buildActionSpec(resizeInfo, resize), contextId);

        assertEquals(ActionType.RESIZE, actionApiDTO.getActionType());
        assertTrue(actionApiDTO.getDetails().contains(expectedDetailCapacityy));
        assertEquals(CommodityDTO.CommodityType.VMEM.name(),
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
                        .setCommodityType(commodityMem))
                .build();

        Explanation resize = Explanation.newBuilder()
                .setResize(ResizeExplanation.newBuilder()
                        .setStartUtilization(0.2f)
                        .setEndUtilization(0.4f).build())
                .build();

        Mockito.when(searchMole.searchPlanTopologyEntityDTOs(any()))
            .thenReturn(SearchPlanTopologyEntityDTOsResponse.newBuilder()
                .addAllTopologyEntityDtos(Lists.newArrayList(
                    topologyEntityDTO(ENTITY_TO_RESIZE_NAME, targetId, EntityType.VIRTUAL_MACHINE_VALUE)
                )).build());

        final ActionApiDTO actionApiDTO =
                mapper.mapActionSpecToActionApiDTO(buildActionSpec(resizeInfo, resize), contextId);
        assertEquals(ENTITY_TO_RESIZE_NAME, actionApiDTO.getTarget().getDisplayName());
        assertEquals(ActionType.RESIZE, actionApiDTO.getActionType());
        assertEquals(CommodityDTO.CommodityType.MEM.name(),
                actionApiDTO.getRisk().getReasonCommodity());
        // Verify that we recognize this as a mem limit removal. We'll just check the start of the
        // string, there isn't much point to checking the entity-describing remainder of the
        // string that follows.
        final String REMOVE_DESCRIPTION = "Remove Mem limit on entity";
        assertTrue(actionApiDTO.getDetails().startsWith(REMOVE_DESCRIPTION));
    }

    @Test
    public void testMapActivate() throws Exception {
        final long targetId = 1;
        final ActionInfo activateInfo = ActionInfo.newBuilder()
                        .setActivate(Activate.newBuilder().setTarget(ApiUtilsTest.createActionEntity(targetId))
                                        .addTriggeringCommodities(commodityCpu)
                                        .addTriggeringCommodities(commodityMem))
                        .build();
        Explanation activate =
                        Explanation.newBuilder()
                                        .setActivate(ActivateExplanation.newBuilder()
                                                        .setMostExpensiveCommodity(commodityCpu.getType()).build())
                                        .build();
        Mockito.when(searchMole.searchPlanTopologyEntityDTOs(any()))
            .thenReturn(SearchPlanTopologyEntityDTOsResponse.newBuilder()
                .addAllTopologyEntityDtos(Lists.newArrayList(
                    topologyEntityDTO("EntityToActivate", targetId, EntityType.VIRTUAL_MACHINE_VALUE)
                )).build());

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
                buildActionSpec(activateInfo, activate), contextId);
        assertEquals("EntityToActivate", actionApiDTO.getTarget().getDisplayName());
        assertEquals(targetId, Long.parseLong(actionApiDTO.getTarget().getUuid()));
        assertEquals(ActionType.START, actionApiDTO.getActionType());
        Assert.assertThat(actionApiDTO.getRisk().getReasonCommodity().split(","),
            IsArrayContainingInAnyOrder.arrayContainingInAnyOrder(
                    CommodityDTO.CommodityType.CPU.name(),
                    CommodityDTO.CommodityType.MEM.name()));
        assertThat(actionApiDTO.getDetails(), containsString("Start Virtual Machine"));
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
                        .addTriggeringCommodities(commodityCpu)
                        .addTriggeringCommodities(commodityMem))
                .build();
        Explanation activate = Explanation.newBuilder()
                .setDeactivate(DeactivateExplanation.newBuilder().build()).build();
        final String entityToActivateName = "EntityToActivate";
        final String className = "Storage";
        final String prettyClassName = "Storage";
        Mockito.when(searchMole.searchPlanTopologyEntityDTOs(any()))
            .thenReturn(SearchPlanTopologyEntityDTOsResponse.newBuilder()
                .addAllTopologyEntityDtos(Lists.newArrayList(
                    topologyEntityDTO(entityToActivateName, targetId, EntityType.STORAGE_VALUE)
                )).build());

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
                buildActionSpec(deactivateInfo, activate), contextId);
        assertEquals(entityToActivateName, actionApiDTO.getTarget().getDisplayName());
        assertEquals(targetId, Long.parseLong(actionApiDTO.getTarget().getUuid()));
        assertEquals(ActionType.ADD_PROVIDER, actionApiDTO.getActionType());
        assertThat(actionApiDTO.getRisk().getReasonCommodity().split(","),
                IsArrayContainingInAnyOrder.arrayContainingInAnyOrder(
                        CommodityDTO.CommodityType.CPU.name(), CommodityDTO.CommodityType.MEM.name()));
        assertThat(actionApiDTO.getDetails(), containsString("Add provider " + prettyClassName ));
    }

    @Test
    public void testMapDeactivate() throws Exception {
        final long targetId = 1;
        final ActionInfo deactivateInfo = ActionInfo.newBuilder()
            .setDeactivate(Deactivate.newBuilder().setTarget(ApiUtilsTest.createActionEntity(targetId))
                .addTriggeringCommodities(commodityCpu)
                .addTriggeringCommodities(commodityMem))
            .build();
        Explanation deactivate = Explanation.newBuilder()
            .setDeactivate(DeactivateExplanation.newBuilder().build()).build();
        final String entityToDeactivateName = "EntityToDeactivate";
        final String prettyClassName = "Virtual Machine";
        Mockito.when(searchMole.searchPlanTopologyEntityDTOs(any()))
            .thenReturn(SearchPlanTopologyEntityDTOsResponse.newBuilder()
                .addAllTopologyEntityDtos(Lists.newArrayList(
                    topologyEntityDTO(entityToDeactivateName, targetId, EntityType.VIRTUAL_MACHINE_VALUE)
                )).build());

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
            buildActionSpec(deactivateInfo, deactivate), contextId);
        assertEquals(entityToDeactivateName, actionApiDTO.getTarget().getDisplayName());
        assertEquals(targetId, Long.parseLong(actionApiDTO.getTarget().getUuid()));
        assertEquals(ActionType.SUSPEND, actionApiDTO.getActionType());
        assertThat(actionApiDTO.getRisk().getReasonCommodity().split(","),
            IsArrayContainingInAnyOrder.arrayContainingInAnyOrder(
                CommodityDTO.CommodityType.CPU.name(), CommodityDTO.CommodityType.MEM.name()));
        assertThat(actionApiDTO.getDetails(), is("Suspend " + prettyClassName +
            " " + entityToDeactivateName));
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
        final String prettyClassName = "Storage";
        Mockito.when(searchMole.searchPlanTopologyEntityDTOs(any()))
            .thenReturn(SearchPlanTopologyEntityDTOsResponse.newBuilder()
                .addAllTopologyEntityDtos(Lists.newArrayList(
                    topologyEntityDTO(entityToDelete, targetId, EntityType.STORAGE_VALUE)
                )).build());

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
            buildActionSpec(deleteInfo, delete), contextId);
        assertEquals(entityToDelete, actionApiDTO.getTarget().getDisplayName());
        assertEquals(targetId, Long.parseLong(actionApiDTO.getTarget().getUuid()));
        assertEquals(ActionType.DELETE, actionApiDTO.getActionType());
        assertThat(actionApiDTO.getDetails(), is("Delete wasted file '" + fileName + "' from " + prettyClassName +
            " " + entityToDelete + " to free up 2 MB"));
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
                        .addTriggeringCommodities(commodityCpu)
                        .addTriggeringCommodities(commodityMem))
                .build();
        Explanation deactivate = Explanation.newBuilder()
                .setDeactivate(DeactivateExplanation.newBuilder().build()).build();
        final String entityToDeactivateName = "EntityToDeactivate";
        final String prettyClassName = "Disk Array";
        Mockito.when(searchMole.searchPlanTopologyEntityDTOs(any()))
            .thenReturn(SearchPlanTopologyEntityDTOsResponse.newBuilder()
                .addAllTopologyEntityDtos(Lists.newArrayList(
                    topologyEntityDTO(entityToDeactivateName, targetId, EntityType.DISK_ARRAY_VALUE)
                )).build());
        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
                buildActionSpec(deactivateInfo, deactivate), contextId);
        assertEquals(entityToDeactivateName, actionApiDTO.getTarget().getDisplayName());
        assertEquals(targetId, Long.parseLong(actionApiDTO.getTarget().getUuid()));
        assertEquals(ActionType.DELETE, actionApiDTO.getActionType());
        assertThat(actionApiDTO.getRisk().getReasonCommodity().split(","),
                IsArrayContainingInAnyOrder.arrayContainingInAnyOrder(
                        CommodityDTO.CommodityType.CPU.name(), CommodityDTO.CommodityType.MEM.name()));
        assertThat(actionApiDTO.getDetails(), is("Delete " + prettyClassName +
                " " + entityToDeactivateName));
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
        ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(actionSpec, contextId);

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
                .setCommodityType(commodityCpu))
            .build();

        Mockito.when(searchMole.searchPlanTopologyEntityDTOs(any()))
            .thenReturn(SearchPlanTopologyEntityDTOsResponse.newBuilder()
                .addAllTopologyEntityDtos(Lists.newArrayList(
                    topologyEntityDTO("EntityToResize", goodTarget, EntityType.VIRTUAL_MACHINE_VALUE)
                )).build());

        final ActionSpec moveSpec = buildActionSpec(moveInfo, Explanation.getDefaultInstance(), Optional.empty());
        final ActionSpec resizeSpec = buildActionSpec(resizeInfo, Explanation.getDefaultInstance(), Optional.empty());

        final List<ActionApiDTO> dtos = mapper.mapActionSpecsToActionApiDTOs(
                Arrays.asList(moveSpec, resizeSpec), contextId);
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
        Mockito.when(searchMole.searchPlanTopologyEntityDTOs(any()))
            .thenReturn(SearchPlanTopologyEntityDTOsResponse.newBuilder()
                .addAllTopologyEntityDtos(Lists.newArrayList(
                    topologyEntityDTO("target", 1, EntityType.VIRTUAL_MACHINE_VALUE),
                    topologyEntityDTO("source", 2, EntityType.VIRTUAL_MACHINE_VALUE),
                    topologyEntityDTO("dest", 3, EntityType.VIRTUAL_MACHINE_VALUE)
                )).build());
        final Compliance compliance = Compliance.newBuilder().addMissingCommodities(
                        CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
                                        .setKey(String.valueOf(POLICY_ID))
                                        .build()).build();
        final MoveExplanation moveExplanation = MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                        .setCompliance(compliance).build())
                        .build();
        final List<ActionApiDTO> dtos = mapper.mapActionSpecsToActionApiDTOs(
                        Arrays.asList(buildActionSpec(moveInfo, Explanation.newBuilder()
                                        .setMove(moveExplanation).build())),
                        contextId);
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
        Mockito.when(searchMole.searchPlanTopologyEntityDTOs(any()))
            .thenReturn(SearchPlanTopologyEntityDTOsResponse.newBuilder()
                .addAllTopologyEntityDtos(Lists.newArrayList(
                    topologyEntityDTO("target", 1, EntityType.VIRTUAL_MACHINE_VALUE),
                    topologyEntityDTO("source", 2, EntityType.PHYSICAL_MACHINE_VALUE),
                    topologyEntityDTO("dest", 3, EntityType.PHYSICAL_MACHINE_VALUE),
                    topologyEntityDTO("stSource", 4, EntityType.STORAGE_VALUE),
                    topologyEntityDTO("stDest", 5, EntityType.STORAGE_VALUE)
                )).build());

        final Compliance compliance = Compliance.newBuilder().addMissingCommodities(
            CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.SEGMENTATION_VALUE)
                .setKey(String.valueOf(POLICY_ID))
                .build()).build();
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
                .setMove(moveExplanation1).build())), contextId);
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
                .setMove(moveExplanation2).build())), contextId);
        Assert.assertEquals(DEFAULT_EXPLANATION, dtos2.get(0).getRisk().getDescription());
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
                                        .addMissingCommodities(commodityMem)
                                        .addMissingCommodities(commodityCpu)
                                        .build())
                                .build())
                        .build())
                .build();
        Mockito.when(searchMole.searchTopologyEntityDTOs(any()))
            .thenReturn(SearchTopologyEntityDTOsResponse.newBuilder()
                .addAllTopologyEntityDtos(Lists.newArrayList(
                    topologyEntityDTO("target", 1, EntityType.VIRTUAL_MACHINE_VALUE),
                    topologyEntityDTO("source", 2, EntityType.VIRTUAL_MACHINE_VALUE),
                    topologyEntityDTO("dest", 3, EntityType.VIRTUAL_MACHINE_VALUE)
                )).build());
        final ActionApiDTO actionApiDTO =
                mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, compliance), contextId);
        assertEquals(com.vmturbo.api.enums.ActionState.SUCCEEDED, actionApiDTO.getActionState());

        final ActionApiDTO realTimeActionApiDTO =
                mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, compliance), REAL_TIME_TOPOLOGY_CONTEXT_ID);
        assertEquals(com.vmturbo.api.enums.ActionState.PENDING_ACCEPT, realTimeActionApiDTO.getActionState());

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
        // This special case should get mapped to the RECOMMENDED state.
        // This will make it non-selectable in the UI!
        assertThat(actionApiDTO.getActionState(), is(com.vmturbo.api.enums.ActionState.RECOMMENDED));
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
        // This special case should get mapped to the RECOMMENDED state.
        // This will make it non-selectable in the UI!
        assertThat(actionApiDTO.getActionState(), is(com.vmturbo.api.enums.ActionState.RECOMMENDED));
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
        // This special case should get mapped to the RECOMMENDED state.
        // This will make it non-selectable in the UI!
        assertThat(actionApiDTO.getActionState(), is(com.vmturbo.api.enums.ActionState.PENDING_ACCEPT));
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
        // This special case should get mapped to the RECOMMENDED state.
        // This will make it non-selectable in the UI!
        assertThat(actionApiDTO.getActionState(), is(com.vmturbo.api.enums.ActionState.RECOMMENDED));
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
        // This special case should get mapped to the RECOMMENDED state.
        // This will make it non-selectable in the UI!
        assertThat(actionApiDTO.getActionState(), is(com.vmturbo.api.enums.ActionState.QUEUED));
    }

    @Test
    public void testCreateActionFilterNoInvolvedEntities() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        final Optional<Set<Long>> involvedEntities = Optional.empty();

        final ActionQueryFilter filter = mapper.createActionFilter(inputDto, involvedEntities);

        assertFalse(filter.hasInvolvedEntities());
    }

    @Test
    public void testCreateActionFilterDates() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        inputDto.setStartTime(DateTimeUtil.toString(1_000_000));
        inputDto.setEndTime(DateTimeUtil.toString(2_000_000));

        final ActionQueryFilter filter = mapper.createActionFilter(inputDto, Optional.empty());
        assertThat(filter.getStartDate(), is(1_000_000L));
        assertThat(filter.getEndDate(), is(2_000_000L));
    }

    @Test
    public void testCreateLiveActionFilterIgnoresDates() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        inputDto.setStartTime(DateTimeUtil.toString(1_000_000));
        inputDto.setEndTime(DateTimeUtil.toString(1_000_000));

        final ActionQueryFilter filter = mapper.createLiveActionFilter(inputDto, Optional.empty());
        assertFalse(filter.hasStartDate());
        assertFalse(filter.hasEndDate());
    }

    @Test
    public void testCreateActionFilterEnvTypeCloud() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        inputDto.setEnvironmentType(EnvironmentType.CLOUD);

        final ActionQueryFilter filter = mapper.createLiveActionFilter(inputDto, Optional.empty());
        assertThat(filter.getEnvironmentType(), is(EnvironmentTypeEnum.EnvironmentType.CLOUD));
    }

    @Test
    public void testCreateActionFilterEnvTypeOnPrem() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        inputDto.setEnvironmentType(EnvironmentType.ONPREM);

        final ActionQueryFilter filter = mapper.createLiveActionFilter(inputDto, Optional.empty());
        assertThat(filter.getEnvironmentType(), is(EnvironmentTypeEnum.EnvironmentType.ON_PREM));
    }

    @Test
    public void testCreateActionFilterEnvTypeUnset() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();

        final ActionQueryFilter filter = mapper.createLiveActionFilter(inputDto, Optional.empty());
        assertFalse(filter.hasEnvironmentType());
    }

    @Test
    public void testCreateActionFilterWithInvolvedEntities() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        final Set<Long> oids = Sets.newHashSet(1L, 2L, 3L);
        final Optional<Set<Long>> involvedEntities = Optional.of(oids);

        final ActionQueryFilter filter = mapper.createActionFilter(inputDto, involvedEntities);

        assertTrue(filter.hasInvolvedEntities());
        assertEquals(new HashSet<Long>(oids),
                     new HashSet<Long>(filter.getInvolvedEntities().getOidsList()));
    }

    // The UI request for "Pending Actions" does not include any action states
    // in its filter even though it wants to exclude executed actions. When given
    // no action states we should automatically insert the operational action states.
    @Test
    public void testCreateActionFilterWithNoStateFilter() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();

        final ActionQueryFilter filter = mapper.createActionFilter(inputDto, Optional.empty());
        Assert.assertThat(filter.getStatesList(),
            containsInAnyOrder(ActionSpecMapper.OPERATIONAL_ACTION_STATES));
    }

    // Similar fixes as in OM-24590: Do not show executed actions as pending,
    // when "inputDto" is null, we should automatically insert the operational action states.
    @Test
    public void testCreateActionFilterWithNoStateFilterAndNoInputDTO() {
        final ActionQueryFilter filter = mapper.createActionFilter(null, Optional.empty());
        Assert.assertThat(filter.getStatesList(),
                containsInAnyOrder(ActionSpecMapper.OPERATIONAL_ACTION_STATES));
    }

    @Test
    public void testTranslateExplanation() {
        Map<Long, TopologyEntityDTO> entitiesMap = new HashMap<>();
        TopologyEntityDTO entity = TopologyEntityDTO.newBuilder()
            .setDisplayName("Test Entity")
            .setOid(1L)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .build();
        entitiesMap.put(1L, entity);
        ActionSpecMappingContext context = new ActionSpecMappingContext(entitiesMap,
            Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
            Collections.emptyMap(), Collections.emptyMap());
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
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        List<com.vmturbo.api.enums.ActionState> actionStates = new ArrayList<>();
        actionStates.add(com.vmturbo.api.enums.ActionState.REJECTED);
        inputDto.setActionStateList(actionStates);
        mapper.createActionFilter(inputDto, Optional.empty());
    }

    @Test (expected = IllegalArgumentException.class)
    public void testAccountingStateMustReturnError() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        List<com.vmturbo.api.enums.ActionState> actionStates = new ArrayList<>();
        actionStates.add(com.vmturbo.api.enums.ActionState.ACCOUNTING);
        inputDto.setActionStateList(actionStates);
        mapper.createActionFilter(inputDto, Optional.empty());
    }

    @Test
    public void testMapPostInProgress(){
        ActionSpec.Builder builder = ActionSpec.newBuilder()
            .setActionState(ActionState.POST_IN_PROGRESS);
        ActionSpec actionSpec = builder.build();
        com.vmturbo.api.enums.ActionState actionState = mapper.calculateApiActionState(actionSpec);
        assertThat(actionState, is(com.vmturbo.api.enums.ActionState.IN_PROGRESS));
    }

    @Test
    public void testMapPreInProgress(){
        ActionSpec.Builder builder = ActionSpec.newBuilder()
            .setActionState(ActionState.PRE_IN_PROGRESS);
        ActionSpec actionSpec = builder.build();
        com.vmturbo.api.enums.ActionState actionState = mapper.calculateApiActionState(actionSpec);
        assertThat(actionState, is(com.vmturbo.api.enums.ActionState.IN_PROGRESS));
    }

    private ActionInfo getHostMoveActionInfo() {
        return getMoveActionInfo(UIEntityType.PHYSICAL_MACHINE.getValue(), true);
    }

    private ActionInfo getStorageMoveActionInfo() {
        return getMoveActionInfo(UIEntityType.STORAGE.getValue(), true);
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

        Mockito.when(searchMole.searchTopologyEntityDTOs(any()))
            .thenReturn(SearchTopologyEntityDTOsResponse.newBuilder()
                .addAllTopologyEntityDtos(Lists.newArrayList(
                    topologyEntityDTO(TARGET, 3L, EntityType.VIRTUAL_MACHINE_VALUE),
                    topologyEntityDTO(SOURCE, 1L, ServiceEntityMapper.fromUIEntityType(srcAndDestType)),
                    topologyEntityDTO(DESTINATION, 2L, ServiceEntityMapper.fromUIEntityType(srcAndDestType))
                )).build());

        Mockito.when(searchMole.searchPlanTopologyEntityDTOs(any()))
            .thenReturn(SearchPlanTopologyEntityDTOsResponse.newBuilder()
                .addAllTopologyEntityDtos(Lists.newArrayList(
                    topologyEntityDTO(TARGET, 3L, EntityType.VIRTUAL_MACHINE_VALUE),
                    topologyEntityDTO(SOURCE, 1L, ServiceEntityMapper.fromUIEntityType(srcAndDestType)),
                    topologyEntityDTO(DESTINATION, 2L, ServiceEntityMapper.fromUIEntityType(srcAndDestType))
                )).build());

        return moveInfo;
    }

    /**
     * Build a map from OID (a Long) to a optional of {@link ServiceEntityApiDTO} with that OID.
     *
     * note:  the returned map is Immutable.
     *
     * @param dtos an array of {@link ServiceEntityApiDTO} to put into the map
     * @return an {@link ImmutableMap} from OID to {@link ServiceEntityApiDTO}
     */
    private Map<Long, Optional<ServiceEntityApiDTO>> oidToEntityMap(
            ServiceEntityApiDTO ...dtos) {

        Map<Long, Optional<ServiceEntityApiDTO>> answer = new HashMap<>();

        for (ServiceEntityApiDTO dto : dtos) {
            answer.put(Long.valueOf(dto.getUuid()), Optional.of(dto));
        }
        return answer;
    }


    /**
     * Create a new instances of {@link ServiceEntityApiDTO} and initialize the displayName,
     * uuid, and class name fields.
     *
     * @param displayName the displayName for the new SE
     * @param oid the OID, to be converted to String and set as the uuid
     * @param className the class name for the new SE
     * @return a service entity DTO
     */
    private ServiceEntityApiDTO entityApiDTO(@Nonnull final String displayName, long oid,
                                             @Nonnull String className) {
        ServiceEntityApiDTO seDTO = new ServiceEntityApiDTO();
        seDTO.setDisplayName(displayName);
        seDTO.setUuid(Long.toString(oid));
        seDTO.setClassName(className);
        return seDTO;
    }

    private TopologyEntityDTO topologyEntityDTO(@Nonnull final String displayName, long oid,
                                                int entityType) {
        return TopologyEntityDTO.newBuilder()
            .setOid(oid)
            .setDisplayName(displayName)
            .setEntityType(entityType)
            .build();
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
            .setExplanation(DEFAULT_EXPLANATION);

        decision.ifPresent(builder::setDecision);
        return builder.build();
    }

    private Action buildAction(ActionInfo actionInfo, Explanation explanation) {
        return Action.newBuilder()
            .setImportance(0)
            .setId(1234)
            .setInfo(actionInfo)
            .setExplanation(explanation)
            .build();
    }
}
