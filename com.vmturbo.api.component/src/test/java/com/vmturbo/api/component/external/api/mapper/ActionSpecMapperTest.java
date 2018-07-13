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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.component.external.api.util.ApiUtilsTest;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ActivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.InitialPlacement;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeactivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Unit tests for {@link ActionSpecMapper}.
 */
public class ActionSpecMapperTest {

    public static final int POLICY_ID = 10;
    public static final String POLICY_NAME = "policy";
    private ActionSpecMapper mapper;

    private RepositoryApi repositoryApi;

    private PolicyDTOMoles.PolicyServiceMole policyMole;

    private GrpcTestServer grpcServer;

    private PolicyServiceGrpc.PolicyServiceBlockingStub policyService;

    private final long contextId = 777L;

    private CommodityType commodityCpu;
    private CommodityType commodityMem;
    private CommodityType commodityVMem;

    private static final String START = "Start";
    private static final String TARGET = "Target";
    private static final String SOURCE = "Source";
    private static final String DESTINATION = "Destination";

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
        mapper = new ActionSpecMapper(repositoryApi, policyService, Executors
                        .newCachedThreadPool(new ThreadFactoryBuilder().build()));
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
        assertEquals("0", actionApiDTO.getTarget().getUuid());

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
        assertEquals("0", actionApiDTO.getTarget().getUuid());

        ActionApiDTO first = actionApiDTO.getCompoundActions().get(0);
        assertEquals(null, first.getCurrentEntity().getDisplayName());
        assertEquals(null, first.getCurrentValue());

        assertEquals(DESTINATION, first.getNewEntity().getDisplayName());
        assertEquals("2", first.getNewValue());

        assertEquals(ActionType.START, actionApiDTO.getActionType());
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
        assertEquals("0", actionApiDTO.getTarget().getUuid());

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
        assertEquals("0", actionApiDTO.getTarget().getUuid());

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
        assertEquals("0", actionApiDTO.getTarget().getUuid());

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

        ActionInfo moveInfo =
                    ActionInfo.newBuilder().setReconfigure(
                            Reconfigure.newBuilder()
                            .setTarget(ApiUtilsTest.createActionEntity(0))
                            .setSource(ApiUtilsTest.createActionEntity(1))
                            .build())
                    .build();
        Explanation reconfigure =
                    Explanation.newBuilder()
                            .setReconfigure(ReconfigureExplanation.newBuilder()
                                    .addReconfigureCommodity(cpuAllocation).build())
                            .build();
        Mockito.when(repositoryApi.getServiceEntitiesById(any()))
                        .thenReturn(oidToEntityMap(
                                entityApiDTO(TARGET, 0L, "C0"),
                                entityApiDTO(SOURCE, 1L, "C1")));

        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, reconfigure), contextId);
        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("0", actionApiDTO.getTarget().getUuid());
        assertEquals("C0", actionApiDTO.getTarget().getClassName());

        assertEquals(SOURCE, actionApiDTO.getCurrentEntity().getDisplayName());
        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("1", actionApiDTO.getCurrentValue());

        assertEquals( ActionType.RECONFIGURE, actionApiDTO.getActionType());
        assertEquals(
            "Reconfigure C 0 'Target' which requires Cpu Allocation but is hosted by C 1 'Source' " +
                "which does not provide Cpu Allocation",
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
                            .setTarget(ApiUtilsTest.createActionEntity(0))
                            .build())
                    .build();
        Explanation reconfigure =
                    Explanation.newBuilder()
                            .setReconfigure(ReconfigureExplanation.newBuilder()
                                    .addReconfigureCommodity(cpuAllocation).build())
                            .build();
        Mockito.when(repositoryApi.getServiceEntitiesById(any()))
                        .thenReturn(oidToEntityMap(
                                entityApiDTO(TARGET, 0L, "C0")));

        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveInfo, reconfigure), contextId);
        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());
        assertEquals("0", actionApiDTO.getTarget().getUuid());
        assertEquals("C0", actionApiDTO.getTarget().getClassName());

        assertEquals(TARGET, actionApiDTO.getTarget().getDisplayName());

        assertEquals( ActionType.RECONFIGURE, actionApiDTO.getActionType());
        assertEquals(
            "Reconfigure C 0 'Target' as it is unplaced.",
            actionApiDTO.getDetails());
    }

    @Test
    public void testMapProvision() throws Exception {
        ActionInfo provisionInfo =
                ActionInfo.newBuilder()
                    .setProvision(Provision.newBuilder()
                        .setEntityToClone(ApiUtilsTest.createActionEntity(0))
                        .setProvisionedSeller(-1).build()).build();
        Explanation provision = Explanation.newBuilder().setProvision(ProvisionExplanation
                        .newBuilder().setProvisionBySupplyExplanation(ProvisionBySupplyExplanation
                                        .newBuilder().setMostExpensiveCommodity(21).build())
                        .build()).build();
        Mockito.when(repositoryApi.getServiceEntitiesById(any()))
            .thenReturn(oidToEntityMap(entityApiDTO("EntityToClone", 0L, "c0")));

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
                buildActionSpec(provisionInfo, provision), contextId);
        assertEquals("EntityToClone", actionApiDTO.getCurrentEntity().getDisplayName());
        assertEquals("0", actionApiDTO.getCurrentValue());

        assertEquals("New Entity", actionApiDTO.getTarget().getDisplayName());
        assertEquals("c0", actionApiDTO.getTarget().getClassName());
        assertEquals("-1", actionApiDTO.getTarget().getUuid());

        assertEquals("New Entity", actionApiDTO.getNewEntity().getDisplayName());
        assertEquals("c0", actionApiDTO.getNewEntity().getClassName());
        assertEquals("-1", actionApiDTO.getNewEntity().getUuid());

        assertEquals(ActionType.PROVISION, actionApiDTO.getActionType());
        assertThat(actionApiDTO.getDetails(), containsString("Provision c 0 'EntityToClone'"));
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

        Mockito.when(repositoryApi.getServiceEntitiesById(any()))
            .thenReturn(oidToEntityMap(entityApiDTO("EntityToResize", targetId, "c0")));

        final ActionApiDTO actionApiDTO =
            mapper.mapActionSpecToActionApiDTO(buildActionSpec(resizeInfo, resize), contextId);
        assertEquals("EntityToResize", actionApiDTO.getTarget().getDisplayName());
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
        Mockito.when(repositoryApi.getServiceEntitiesById(any()))
                .thenReturn(oidToEntityMap(entityApiDTO("EntityToResize", targetId, "c0")));

        final ActionApiDTO actionApiDTO =
                mapper.mapActionSpecToActionApiDTO(buildActionSpec(resizeInfo, resize), contextId);

        assertEquals(ActionType.RESIZE, actionApiDTO.getActionType());
        assertTrue(actionApiDTO.getDetails().contains(expectedDetailCapacityy));
        assertEquals(CommodityDTO.CommodityType.VMEM.name(),
                actionApiDTO.getRisk().getReasonCommodity());
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
        Mockito.when(repositoryApi.getServiceEntitiesById(any()))
                        .thenReturn(oidToEntityMap(
                                entityApiDTO("EntityToActivate", targetId, "c0")));

        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
                buildActionSpec(activateInfo, activate), contextId);
        assertEquals("EntityToActivate", actionApiDTO.getTarget().getDisplayName());
        assertEquals(targetId, Long.parseLong(actionApiDTO.getTarget().getUuid()));
        assertEquals(ActionType.START, actionApiDTO.getActionType());
        Assert.assertThat(actionApiDTO.getRisk().getReasonCommodity().split(","),
            IsArrayContainingInAnyOrder.arrayContainingInAnyOrder(
                    CommodityDTO.CommodityType.CPU.name(),
                    CommodityDTO.CommodityType.MEM.name()));
        assertThat(actionApiDTO.getDetails(), containsString("Start c 0"));
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
        Mockito.when(repositoryApi.getServiceEntitiesById(any()))
                .thenReturn(oidToEntityMap(entityApiDTO(entityToActivateName, targetId, className)));


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
        final String className = "C0";
        final String prettyClassName = "C 0";
        Mockito.when(repositoryApi.getServiceEntitiesById(any()))
            .thenReturn(oidToEntityMap(entityApiDTO(entityToDeactivateName, targetId, className)));


        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
            buildActionSpec(deactivateInfo, deactivate), contextId);
        assertEquals(entityToDeactivateName, actionApiDTO.getTarget().getDisplayName());
        assertEquals(targetId, Long.parseLong(actionApiDTO.getTarget().getUuid()));
        assertEquals(ActionType.SUSPEND, actionApiDTO.getActionType());
        assertThat(actionApiDTO.getRisk().getReasonCommodity().split(","),
                        IsArrayContainingInAnyOrder.arrayContainingInAnyOrder(
                                        CommodityDTO.CommodityType.CPU.name(), CommodityDTO.CommodityType.MEM.name()));
        assertThat(actionApiDTO.getDetails(), is("Suspend " + prettyClassName +
                " '" + entityToDeactivateName + "'."));
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
        final String className = "DiskArray";
        final String prettyClassName = "Disk Array";
        Mockito.when(repositoryApi.getServiceEntitiesById(any()))
                .thenReturn(oidToEntityMap(entityApiDTO(entityToDeactivateName, targetId, className)));


        final ActionApiDTO actionApiDTO = mapper.mapActionSpecToActionApiDTO(
                buildActionSpec(deactivateInfo, deactivate), contextId);
        assertEquals(entityToDeactivateName, actionApiDTO.getTarget().getDisplayName());
        assertEquals(targetId, Long.parseLong(actionApiDTO.getTarget().getUuid()));
        assertEquals(ActionType.DELETE, actionApiDTO.getActionType());
        assertThat(actionApiDTO.getRisk().getReasonCommodity().split(","),
                IsArrayContainingInAnyOrder.arrayContainingInAnyOrder(
                        CommodityDTO.CommodityType.CPU.name(), CommodityDTO.CommodityType.MEM.name()));
        assertThat(actionApiDTO.getDetails(), is("Delete " + prettyClassName +
                " '" + entityToDeactivateName + "'."));
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
        final long badTarget = 0L;
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

        final Map<Long, Optional<ServiceEntityApiDTO>> involvedEntities = oidToEntityMap(
            entityApiDTO("EntityToResize", goodTarget, "c0"));
        involvedEntities.put(badTarget, Optional.empty());
        involvedEntities.put(badSource, Optional.empty());
        involvedEntities.put(badDestination, Optional.empty());

        Mockito.when(repositoryApi.getServiceEntitiesById(any()))
            .thenReturn(involvedEntities);

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
        final Map<Long, Optional<ServiceEntityApiDTO>> involvedEntities = oidToEntityMap(
                        entityApiDTO("target", 1, "VM"),
                        entityApiDTO("source", 2, "VM"),
                        entityApiDTO("dest", 3, "VM")
        );
        Mockito.when(repositoryApi.getServiceEntitiesById(any()))
                        .thenReturn(involvedEntities);
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
    public void testCreateActionFilterNoInvolvedEntities() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        final Optional<Collection<Long>> involvedEntities = Optional.empty();

        final ActionQueryFilter filter = mapper.createActionFilter(inputDto, involvedEntities);

        assertFalse(filter.hasInvolvedEntities());
    }

    @Test
    public void testCreateActionFilterWithInvolvedEntities() {
        final ActionApiInputDTO inputDto = new ActionApiInputDTO();
        final Collection<Long> oids = Arrays.asList(1L, 2L, 3L);
        final Optional<Collection<Long>> involvedEntities = Optional.of(oids);

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
                .setTarget(ApiUtilsTest.createActionEntity(0))
                .addChanges(changeProvider)
                .build();

        ActionInfo moveInfo = ActionInfo.newBuilder().setMove(move).build();

        Mockito.when(repositoryApi.getServiceEntitiesById(any()))
                .thenReturn(oidToEntityMap(
                                entityApiDTO(TARGET, 0L, UIEntityType.VIRTUAL_MACHINE.getValue()),
                                entityApiDTO(SOURCE, 1L, srcAndDestType),
                                entityApiDTO(DESTINATION, 2L, srcAndDestType)));
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
            .setExplanation("default explanation");

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
