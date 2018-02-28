package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.hamcrest.collection.IsArrayContainingInAnyOrder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.utils.DateTimeUtil;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Unit tests for {@link ActionSpecMapper}.
 */
public class ActionSpecMapperTest {
    @InjectMocks
    private ActionSpecMapper mapper;

    @Mock
    private RepositoryApi repositoryApi;

    private final long contextId = 777L;

    private CommodityType commodityCpu;
    private CommodityType commodityMem;

    private static final String START = "Start";
    private static final String TARGET = "Target";
    private static final String SOURCE = "Source";
    private static final String DESTINATION = "Destination";

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        commodityCpu = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.CPU_VALUE)
            .setKey("blah")
            .build();
        commodityMem = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.MEM_VALUE)
            .setKey("grah")
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

    @Test
    public void testMapReconfigure() throws Exception {
        final CommodityType cpuAllocation = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.CPU_ALLOCATION_VALUE)
            .build();

        ActionInfo moveInfo = ActionInfo.newBuilder().setReconfigure(
                        Reconfigure.newBuilder().setTargetId(0).setSourceId(1).build()).build();
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
    public void testMapProvision() throws Exception {
        ActionInfo provisionInfo = ActionInfo.newBuilder().setProvision(Provision.newBuilder()
                        .setEntityToCloneId(0).setProvisionedSeller(-1).build()).build();
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
    }

    @Test
    public void testMapResize() throws Exception {
        final long targetId = 1;
        final ActionInfo resizeInfo = ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                    .setTargetId(targetId)
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
    public void testMapActivate() throws Exception {
        final long targetId = 1;
        final ActionInfo activateInfo = ActionInfo.newBuilder()
                        .setActivate(Activate.newBuilder().setTargetId(targetId)
                                        .addTriggeringCommodities(commodityCpu)
                                        .addTriggeringCommodities(commodityMem))
                        .build();
        Explanation activate =
                        Explanation.newBuilder()
                                        .setActivate(ActivateExplanation.newBuilder()
                                                        .setMostExpensiveCommodity(commodityCpu).build())
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
    }

    @Test
    public void testMapDeactivate() throws Exception {
        final long targetId = 1;
        final ActionInfo deactivateInfo = ActionInfo.newBuilder()
                        .setDeactivate(Deactivate.newBuilder().setTargetId(targetId)
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
        assertEquals(ActionType.DEACTIVATE, actionApiDTO.getActionType());
        assertThat(actionApiDTO.getRisk().getReasonCommodity().split(","),
                        IsArrayContainingInAnyOrder.arrayContainingInAnyOrder(
                                        CommodityDTO.CommodityType.CPU.name(), CommodityDTO.CommodityType.MEM.name()));
        assertThat(actionApiDTO.getDetails(), is("Deactivate " + prettyClassName +
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
                .setTargetId(badTarget)
                .addChanges(ChangeProvider.newBuilder()
                    .setSourceId(badSource)
                    .setDestinationId(badDestination)
                    .build())
                .build())
        .build();

        final ActionInfo resizeInfo = ActionInfo.newBuilder()
            .setResize(Resize.newBuilder()
                .setTargetId(goodTarget)
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

    private ActionInfo getHostMoveActionInfo() {
        return getMoveActionInfo(UIEntityType.PHYSICAL_MACHINE.getValue());
    }

    private ActionInfo getStorageMoveActionInfo() {
        return getMoveActionInfo(UIEntityType.STORAGE.getValue());
    }

    private ActionInfo getMoveActionInfo(final String srcAndDestType) {
        ActionInfo moveInfo = ActionInfo.newBuilder().setMove(Move.newBuilder()
            .setTargetId(0)
            .addChanges(ChangeProvider.newBuilder()
                .setSourceId(1)
                .setDestinationId(2)
                .build())
            .build())
        .build();

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
