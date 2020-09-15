package com.vmturbo.common.protobuf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.Allocate;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AtomicResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.BuyRIExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Congestion;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Efficiency;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.InitialPlacement;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation.CommodityMaxAmountAvailableEntry;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ScaleExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.InvolvedEntityCalculation;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link ActionDTOUtil}.
 */
public class ActionDTOUtilTest {

    private static final long TARGET = 10L;

    private static final long SOURCE_1 = 100L;

    private static final long DEST_1 = 101L;

    private static final long SOURCE_2 = 200L;

    private static final long DEST_2 = 201L;

    private static final long COMPUTE_TIER_ID = 301L;

    private static final long REGION_ID = 302L;

    private static final long MASTER_ACCOUNT_ID = 303L;

    private static final Action action = Action.newBuilder()
        .setId(1L)
        .setDeprecatedImportance(1.0)
        .setInfo(ActionInfo.newBuilder()
            .setMove(Move.newBuilder()
                .setTarget(createActionEntity(TARGET))
                .addChanges(ChangeProvider.newBuilder()
                    .setSource(createActionEntity(SOURCE_1))
                    .setDestination(createActionEntity(DEST_1))
                    .build())
                .addChanges(ChangeProvider.newBuilder()
                    .setSource(createActionEntity(SOURCE_2))
                    .setDestination(createActionEntity(DEST_2))
                    .build())
                .build())
            .build())
        .setExplanation(Explanation.newBuilder()
            .setMove(MoveExplanation.newBuilder()
                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                    .setInitialPlacement(InitialPlacement.getDefaultInstance())
                    .build())
                .build())
            .build())
        .build();

    private static final Action moveCompliance = Action.newBuilder()
        .setId(1L)
        .setDeprecatedImportance(1.0)
        .setInfo(ActionInfo.newBuilder()
            .setMove(Move.newBuilder()
                .setTarget(createActionEntity(TARGET))
                .addChanges(ChangeProvider.newBuilder()
                    .setSource(createActionEntity(SOURCE_1))
                    .setDestination(createActionEntity(DEST_1))
                    .build())
                .addChanges(ChangeProvider.newBuilder()
                    .setSource(createActionEntity(SOURCE_2))
                    .setDestination(createActionEntity(DEST_2))
                    .build())
                .build())
            .build())
        .setExplanation(Explanation.newBuilder()
            .setMove(MoveExplanation.newBuilder()
                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                    .setCompliance(Compliance.getDefaultInstance())
                    .build())
                .build())
            .build())
        .build();

    private static final Action scaleAction = Action.newBuilder()
        .setId(1L)
        .setDeprecatedImportance(1.0)
        .setInfo(ActionInfo.newBuilder()
            .setScale(Scale.newBuilder()
                .setTarget(createActionEntity(TARGET))
                .addChanges(ChangeProvider.newBuilder()
                    .setSource(createActionEntity(SOURCE_1))
                    .setDestination(createActionEntity(DEST_1))
                    .build())
                .addChanges(ChangeProvider.newBuilder()
                    .setSource(createActionEntity(SOURCE_2))
                    .setDestination(createActionEntity(DEST_2))
                    .build())
                .build())
            .build())
        .setExplanation(Explanation.newBuilder()
            .setScale(ScaleExplanation.newBuilder()
                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                    .setInitialPlacement(InitialPlacement.getDefaultInstance())
                    .build())
                .build())
            .build())
        .build();

    private static final Action deleteCloudStorageAction = Action.newBuilder()
        .setId(9L)
        .setDeprecatedImportance(1.0)
        .setInfo(ActionInfo.newBuilder()
            .setDelete(Delete.newBuilder()
                .setTarget(createActionEntity(TARGET))
                .setSource(createActionEntity(SOURCE_1))
                .build())
            .build())
        .setExplanation(Explanation.newBuilder()
            .setDelete(DeleteExplanation.newBuilder().build())
            .build())
        .build();

    private static final Action buyRIAction = Action.newBuilder()
        .setId(1L)
        .setDeprecatedImportance(1.0)
        .setInfo(ActionInfo.newBuilder()
            .setBuyRi(BuyRI.newBuilder()
                .setComputeTier(createActionEntity(COMPUTE_TIER_ID))
                .setRegion(createActionEntity(REGION_ID))
                .setMasterAccount(createActionEntity(MASTER_ACCOUNT_ID))
                .build())
            .build())
        .setExplanation(Explanation.newBuilder()
            .setBuyRI(BuyRIExplanation.getDefaultInstance())
            .build())
        .build();

    private static final Action mergedResizeAction = Action.newBuilder()
        .setId(1L)
        .setDeprecatedImportance(1.0)
        .setInfo(ActionInfo.newBuilder()
            .setAtomicResize(AtomicResize.newBuilder()
                .setExecutionTarget(createActionEntity(TARGET))
                .addResizes(ResizeInfo.newBuilder().setTarget(createActionEntity(SOURCE_1)))
                .addResizes(ResizeInfo.newBuilder().setTarget(createActionEntity(SOURCE_2)))
            ))
        .setExplanation(Explanation.newBuilder()
            .setAtomicResize(AtomicResizeExplanation.newBuilder()
                .addPerEntityExplanation(
                    ResizeExplanationPerEntity.newBuilder().addResizeEntityIds(2L).setEntityId("foo"))
                .addAllEntityIds(Arrays.asList(1L))
                .setMergeGroupId("bar")
                .build())
            .build())
                .build();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testCaseConverterUpperHyphen() {
        assertThat(ActionDTOUtil.upperUnderScoreToMixedSpaces("THIS_IS_A_CONSTANT"),
            is("This Is A Constant"));
        assertThat(ActionDTOUtil.mixedSpacesToUpperUnderScore("This Is A Constant"),
            is("THIS_IS_A_CONSTANT"));
    }

    @Test
    public void testCaseConverterUpperNoHyphen() {
        assertThat(ActionDTOUtil.upperUnderScoreToMixedSpaces("NOUNDERSCORES"),
            is("Nounderscores"));
        assertThat(ActionDTOUtil.mixedSpacesToUpperUnderScore("Nounderscores"),
            is("NOUNDERSCORES"));
    }

    @Test
    public void testCaseConverterSpacesAndUnderscores() {
        assertThat(ActionDTOUtil.upperUnderScoreToMixedSpaces("HAS_SPACES_AND_UNDERSCORES"),
            is("Has Spaces And Underscores"));
        assertThat(ActionDTOUtil.mixedSpacesToUpperUnderScore("Has Spaces And Underscores"),
            is("HAS_SPACES_AND_UNDERSCORES"));
    }

    @Test
    public void testCaseConverterMixedCase() {
        assertThat(ActionDTOUtil.upperUnderScoreToMixedSpaces("Mixed_case_some_lOwEr"),
            is("Mixed Case Some Lower"));
    }

    @Test
    public void testMapMoveToStart() {
        assertEquals(ActionType.ACTIVATE, ActionDTOUtil.getActionInfoActionType(action));
    }

    /**
     * Verify that the involved entities of a (compound) Move action are computed correctly.
     *
     * @throws UnsupportedActionException is not supposed to happen
     */
    @Test
    public void testInvolvedEntities() throws UnsupportedActionException {
        Set<Long> involvedEntities = ActionDTOUtil.getInvolvedEntityIds(action);
        assertEquals(Sets.newHashSet(TARGET, SOURCE_1, DEST_1, SOURCE_2, DEST_2), involvedEntities);
    }

    /**
     * Verify that the involved entities for Delete Cloud Storage Action includes both target and source.
     *
     * @throws UnsupportedActionException is not supposed to happen
     */
    @Test
    public void testDeleteStorageInvolvedEntities()  throws UnsupportedActionException {
        Set<Long> involvedEntities = ActionDTOUtil.getInvolvedEntityIds(deleteCloudStorageAction);
        assertEquals(Sets.newHashSet(TARGET, SOURCE_1), involvedEntities);
    }

    /**
     * Verify that the severity entity of a Move action is the source host, when one of the
     * changes involves a host, and the first source provider otherwise.
     *
     * @throws UnsupportedActionException is not supposed to happen
     */
    @Ignore
    @Test
    public void testGetSeverityEntityMove() throws UnsupportedActionException {
        // When SOURCE_1 is a PM and SOURCE_2 is STORAGE, severity entity is SOURCE_1
        assertEquals(SOURCE_1, ActionDTOUtil.getSeverityEntity(action));
        // When SOURCE_2 is a PM and SOURCE_1 is STORAGE, severity entity is SOURCE_2
        assertEquals(SOURCE_2, ActionDTOUtil.getSeverityEntity(action));
        // When all sources are STORAGEs, severity entity is SOURCE_1
        assertEquals(SOURCE_1, ActionDTOUtil.getSeverityEntity(action));
    }

    /**
     * Verify that the severity entity for Move and Scale actions is the target identifier
     * in Cloud environment.
     *
     * @throws UnsupportedActionException is not supposed to happen
     */
    @Test
    public void testGetSeverityCloudEntityForMoveAndScale() throws UnsupportedActionException {
        ActionEntity target = createActionEntity(TARGET, EntityType.VIRTUAL_MACHINE_VALUE, EnvironmentTypeEnum.EnvironmentType.CLOUD);
        ChangeProvider changeProvider = ChangeProvider.newBuilder()
                .setSource(createActionEntity(SOURCE_1, EntityType.COMPUTE_TIER_VALUE, EnvironmentTypeEnum.EnvironmentType.CLOUD))
                .setDestination(createActionEntity(DEST_1, EntityType.COMPUTE_TIER_VALUE, EnvironmentTypeEnum.EnvironmentType.CLOUD))
                .build();
        ActionInfo moveInfo = ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                        .setTarget(target)
                        .addChanges(changeProvider)
                        .build())
                .build();
        ActionInfo scaleInfo = ActionInfo.newBuilder()
                .setScale(Scale.newBuilder()
                        .setTarget(target)
                        .addChanges(changeProvider)
                        .build())
                .build();
        Action cloudMoveAction = createAction(moveInfo);
        Action cloudScaleAction = createAction(scaleInfo);

        assertEquals(TARGET, ActionDTOUtil.getSeverityEntity(cloudMoveAction));
        assertEquals(TARGET, ActionDTOUtil.getSeverityEntity(cloudScaleAction));
    }

    /**
     * Verify that the severity entity for compliance move actions is the target entity.
     *
     * @throws UnsupportedActionException is not supposed to happen
     */
    @Test
    public void testGetSeverityForComplianceMoves() throws UnsupportedActionException {
        ActionEntity target = createActionEntity(TARGET, EntityType.VIRTUAL_MACHINE_VALUE, EnvironmentType.ON_PREM);
        ChangeProvider changeProvider = ChangeProvider.newBuilder()
            .setSource(createActionEntity(SOURCE_1, EntityType.PHYSICAL_MACHINE_VALUE, EnvironmentType.ON_PREM))
            .setDestination(createActionEntity(DEST_1, EntityType.PHYSICAL_MACHINE_VALUE, EnvironmentType.ON_PREM))
            .build();

        ActionInfo moveInfo = ActionInfo.newBuilder()
            .setMove(Move.newBuilder()
                .setTarget(target)
                .addChanges(changeProvider)
                .build())
            .build();

        Action moveAction = Action.newBuilder()
            .setId(123121)
            .setExplanation(Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                    .setIsPrimaryChangeProviderExplanation(true).setCompliance(
                        Compliance.getDefaultInstance()).build()).build()))
            .setInfo(moveInfo)
            .setDeprecatedImportance(1)
            .build();

        assertEquals(TARGET, ActionDTOUtil.getSeverityEntity(moveAction));
    }

    @Test
    public void testReasonCommodityMoveCompliance() {
        final ReasonCommodity commodity1 = createReasonCommodity(1);
        final ReasonCommodity commodity2 = createReasonCommodity(2);

        final Action action = Action.newBuilder()
            .setId(123121)
            .setExplanation(Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setCompliance(Compliance.newBuilder()
                            .addMissingCommodities(commodity1)
                            .addMissingCommodities(commodity2)))))
            .setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                    .setTarget(createActionEntity(11))
                    .addChanges(ChangeProvider.newBuilder()
                        .setSource(createActionEntity(1))
                        .setDestination(createActionEntity(2)))))
            .setDeprecatedImportance(1)
            .build();

        final List<ReasonCommodity> reasonCommodities =
            ActionDTOUtil.getReasonCommodities(action).collect(Collectors.toList());
        assertThat(reasonCommodities, containsInAnyOrder(commodity1, commodity2));
    }

    @Test
    public void testReasonCommodityMoveCongestion() {
        final ReasonCommodity commodity1 = createReasonCommodity(1);
        final ReasonCommodity commodity2 = createReasonCommodity(2);

        final Action action = Action.newBuilder()
            .setId(123121)
            .setExplanation(Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setCongestion(Congestion.newBuilder()
                            .addCongestedCommodities(commodity1)
                            .addCongestedCommodities(commodity2)))))
            .setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                    .setTarget(createActionEntity(11))
                    .addChanges(ChangeProvider.newBuilder()
                        .setSource(createActionEntity(1))
                        .setDestination(createActionEntity(2)))))
            .setDeprecatedImportance(1)
            .build();

        final List<ReasonCommodity> reasonCommodities =
            ActionDTOUtil.getReasonCommodities(action).collect(Collectors.toList());
        assertThat(reasonCommodities, containsInAnyOrder(commodity1, commodity2));
    }

    @Test
    public void testReasonCommodityMoveEfficiency() {
        final ReasonCommodity commodity1 = createReasonCommodity(1);
        final ReasonCommodity commodity2 = createReasonCommodity(2);

        final Action action = Action.newBuilder()
            .setId(123121)
            .setExplanation(Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setEfficiency(Efficiency.newBuilder()
                            .addUnderUtilizedCommodities(commodity2)
                            .addUnderUtilizedCommodities(commodity1)))))
            .setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                    .setTarget(createActionEntity(11))
                    .addChanges(ChangeProvider.newBuilder()
                        .setSource(createActionEntity(1))
                        .setDestination(createActionEntity(2)))))
            .setDeprecatedImportance(1)
            .build();

        final List<ReasonCommodity> reasonCommodities =
            ActionDTOUtil.getReasonCommodities(action).collect(Collectors.toList());
        assertThat(reasonCommodities, containsInAnyOrder(commodity1, commodity2));
    }

    @Test
    public void testReasonCommodityReconfigure() {
        final ReasonCommodity commodity1 = createReasonCommodity(1);
        final ReasonCommodity commodity2 = createReasonCommodity(2);

        final Action action = Action.newBuilder()
            .setId(123121)
            .setExplanation(Explanation.newBuilder()
                .setReconfigure(ReconfigureExplanation.newBuilder()
                    .addReconfigureCommodity(commodity1)
                    .addReconfigureCommodity(commodity2)))
            .setInfo(ActionInfo.newBuilder()
                .setReconfigure(Reconfigure.newBuilder()
                    .setTarget(createActionEntity(11))))
            .setDeprecatedImportance(1)
            .build();

        final List<ReasonCommodity> reasonCommodities =
            ActionDTOUtil.getReasonCommodities(action).collect(Collectors.toList());
        assertThat(reasonCommodities, containsInAnyOrder(commodity1, commodity2));
    }

    @Test
    public void testReasonCommodityProvisionByDemand() {
        final ReasonCommodity commodity1 = createReasonCommodity(1);
        final ReasonCommodity commodity2 = createReasonCommodity(2);

        final Action action = Action.newBuilder()
            .setId(123121)
            .setExplanation(Explanation.newBuilder()
                .setProvision(ProvisionExplanation.newBuilder()
                    .setProvisionByDemandExplanation(ProvisionByDemandExplanation.newBuilder()
                        .setBuyerId(11111)
                        .addCommodityMaxAmountAvailable(CommodityMaxAmountAvailableEntry.newBuilder()
                            .setMaxAmountAvailable(2)
                            .setRequestedAmount(3)
                            .setCommodityBaseType(1))
                        .addCommodityMaxAmountAvailable(CommodityMaxAmountAvailableEntry.newBuilder()
                            .setMaxAmountAvailable(2)
                            .setRequestedAmount(3)
                            .setCommodityBaseType(2)))))
            .setInfo(ActionInfo.newBuilder()
                .setProvision(Provision.newBuilder()
                    .setEntityToClone(createActionEntity(11))))
            .setDeprecatedImportance(1)
            .build();

        final List<ReasonCommodity> reasonCommodities =
            ActionDTOUtil.getReasonCommodities(action).collect(Collectors.toList());
        assertThat(reasonCommodities, containsInAnyOrder(commodity1, commodity2));
    }

    @Test
    public void testReasonCommodityProvisionBySupply() {
        final ReasonCommodity commodity1 = createReasonCommodity(1);

        final Action action = Action.newBuilder()
            .setId(123121)
            .setExplanation(Explanation.newBuilder()
                .setProvision(ProvisionExplanation.newBuilder()
                    .setProvisionBySupplyExplanation(ProvisionBySupplyExplanation.newBuilder()
                        .setMostExpensiveCommodityInfo(commodity1))))
            .setInfo(ActionInfo.newBuilder()
                .setProvision(Provision.newBuilder()
                    .setEntityToClone(createActionEntity(11))))
            .setDeprecatedImportance(1)
            .build();

        final List<ReasonCommodity> reasonCommodities =
            ActionDTOUtil.getReasonCommodities(action).collect(Collectors.toList());
        assertThat(reasonCommodities, containsInAnyOrder(commodity1));
    }

    @Test
    public void testReasonCommodityResize() {
        final ReasonCommodity commodity1 = createReasonCommodity(1);

        final Action action = Action.newBuilder()
            .setId(123121)
            .setExplanation(Explanation.getDefaultInstance())
            .setInfo(ActionInfo.newBuilder()
                .setResize(Resize.newBuilder()
                    .setTarget(createActionEntity(11))
                    .setCommodityType(commodity1.getCommodityType())))
            .setDeprecatedImportance(1)
            .build();

        final List<ReasonCommodity> reasonCommodities =
            ActionDTOUtil.getReasonCommodities(action).collect(Collectors.toList());
        assertThat(reasonCommodities, containsInAnyOrder(commodity1));
    }

    @Test
    public void testReasonCommodityActivate() {
        final ReasonCommodity commodity1 = createReasonCommodity(1);
        final ReasonCommodity commodity2 = createReasonCommodity(2);

        final Action action = Action.newBuilder()
            .setId(123121)
            .setExplanation(Explanation.getDefaultInstance())
            .setInfo(ActionInfo.newBuilder()
                .setActivate(Activate.newBuilder()
                    .setTarget(createActionEntity(11))
                    .addTriggeringCommodities(commodity1.getCommodityType())
                    .addTriggeringCommodities(commodity2.getCommodityType())))
            .setDeprecatedImportance(1)
            .build();

        final List<ReasonCommodity> reasonCommodities =
            ActionDTOUtil.getReasonCommodities(action).collect(Collectors.toList());
        assertThat(reasonCommodities, containsInAnyOrder(commodity1, commodity2));
    }

    @Test
    public void testReasonCommodityDeactivate() {
        final ReasonCommodity commodity1 = createReasonCommodity(1);
        final ReasonCommodity commodity2 = createReasonCommodity(2);

        final Action action = Action.newBuilder()
            .setId(123121)
            .setExplanation(Explanation.getDefaultInstance())
            .setInfo(ActionInfo.newBuilder()
                .setDeactivate(Deactivate.newBuilder()
                    .setTarget(createActionEntity(11))
                    .addTriggeringCommodities(commodity1.getCommodityType())
                    .addTriggeringCommodities(commodity2.getCommodityType())))
            .setDeprecatedImportance(1)
            .build();

        final List<ReasonCommodity> reasonCommodities =
            ActionDTOUtil.getReasonCommodities(action).collect(Collectors.toList());
        assertThat(reasonCommodities, containsInAnyOrder(commodity1, commodity2));
    }

    /**
     * Test for invocation of {@link ActionDTOUtil#getPrimaryEntity(Action, boolean)} method for
     * Move and Scale actions.
     */
    @Test
    public void testGetPrimaryEntityForMoveAndScale() throws UnsupportedActionException {
        final ActionEntity vm = createActionEntity(11);
        final ActionEntity volume = createActionEntity(22);
        final ActionEntity storageTier = createActionEntity(33, EntityType.STORAGE_TIER_VALUE);
        final ChangeProvider changeProvider = ChangeProvider.newBuilder()
                .setSource(storageTier)
                .setDestination(storageTier)
                .setResource(volume)
                .build();
        final ActionInfo moveInfo = ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                        .setTarget(vm)
                        .addChanges(changeProvider))
                .build();
        final Action move = createAction(moveInfo);
        final ActionInfo scaleInfo = ActionInfo.newBuilder()
                .setScale(Scale.newBuilder()
                        .setTarget(vm)
                        .addChanges(changeProvider))
                .build();
        final Action scale = createAction(scaleInfo);

        assertSame(volume, ActionDTOUtil.getPrimaryEntity(move, true));
        assertSame(vm, ActionDTOUtil.getPrimaryEntity(move, false));

        assertSame(vm, ActionDTOUtil.getPrimaryEntity(scale, true));
        assertSame(vm, ActionDTOUtil.getPrimaryEntity(scale, false));
    }

    /**
     * Test for invocation of {@link ActionDTOUtil#getPrimaryEntity(Action)} method for
     * Buy RI action.
     *
     * @throws UnsupportedActionException in case of unknown action type.
     */
    @Test
    public void testGetPrimaryEntityForBuyRI() throws UnsupportedActionException {
        final ActionEntity computeTier = createActionEntity(33, EntityType.COMPUTE_TIER_VALUE);
        final ActionInfo buyRiInfo = ActionInfo.newBuilder()
                .setBuyRi(BuyRI.newBuilder().setComputeTier(computeTier))
                .build();
        final Action buyRi = createAction(buyRiInfo);

        assertSame(computeTier, ActionDTOUtil.getPrimaryEntity(buyRi));
    }

    /**
     * Test for successful invocation of {@link ActionDTOUtil#getChangeProviderList(Action)} method.
     */
    @Test
    public void testGetChangeProviderListSucceeded() {
        final ActionEntity target = createActionEntity(11);
        final ActionInfo move = ActionInfo.newBuilder().setMove(Move.newBuilder()
            .addChanges(ChangeProvider.getDefaultInstance())
            .setTarget(target)).build();
        final ActionInfo rightSize = ActionInfo.newBuilder().setScale(Scale.newBuilder()
            .addChanges(ChangeProvider.getDefaultInstance())
            .setTarget(target)).build();
        Stream.of(move, rightSize).forEach(actionInfo -> {
            final Action action = Action.newBuilder()
                .setId(123121)
                .setExplanation(Explanation.getDefaultInstance())
                .setInfo(actionInfo)
                .setDeprecatedImportance(1)
                .build();
            assertEquals(1, ActionDTOUtil.getChangeProviderList(action).size());
        });
    }

    /**
     * Test for failed invocation of {@link ActionDTOUtil#getChangeProviderList(Action)} method.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetChangeProviderListFailed() {
        final ActionEntity target = createActionEntity(11);
        final Action action = Action.newBuilder()
            .setId(123121)
            .setExplanation(Explanation.getDefaultInstance())
            .setInfo(ActionInfo.newBuilder().setResize(Resize.newBuilder().setTarget(target)))
            .setDeprecatedImportance(1)
            .build();
        ActionDTOUtil.getChangeProviderList(action);
    }

    /**
     * Test for successful invocation of
     * {@link ActionDTOUtil#getChangeProviderExplanationList(Explanation)} method.
     */
    @Test
    public void testGetChangeProviderExplanationListSucceeded() {
        final Explanation move = Explanation.newBuilder()
            .setMove(MoveExplanation.newBuilder()
                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder().build()))
            .build();
        final Explanation rightSize = Explanation.newBuilder()
            .setScale(ScaleExplanation.newBuilder()
                .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder().build()))
            .build();
        Stream.of(move, rightSize).forEach(explanation ->
            assertEquals(1, ActionDTOUtil.getChangeProviderExplanationList(explanation).size()));
    }

    /**
     * Test for successful invocation of
     * {@link ActionDTOUtil#getChangeProviderExplanationList(Explanation)} method when result is
     * empty list.
     */
    @Test
    public void testGetChangeProviderExplanationListEmpty() {
        assertTrue(ActionDTOUtil.getChangeProviderExplanationList(
            Explanation.getDefaultInstance()).isEmpty());
    }

    /**
     * Test for failed invocation of
     * {@link ActionDTOUtil#getChangeProviderExplanationList(Explanation)} method.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetChangeProviderExplanationListFailed() {
        final Explanation explanation = Explanation.newBuilder()
            .setProvision(ProvisionExplanation.getDefaultInstance()).build();
        ActionDTOUtil.getChangeProviderExplanationList(explanation);
    }

    /**
     * Test handling of Allocate action by {@code  ActionDTOUtil} methods.
     * @throws UnsupportedActionException in case of unknown action type.
     */
    @Test
    public void testAllocateAction() throws UnsupportedActionException {
        final ActionEntity vm = createActionEntity(22, EntityType.VIRTUAL_MACHINE_VALUE);
        final ActionEntity computeTier = createActionEntity(33, EntityType.COMPUTE_TIER_VALUE);
        final ActionInfo actionInfo = ActionInfo.newBuilder()
                .setAllocate(Allocate.newBuilder()
                        .setTarget(vm)
                        .setWorkloadTier(computeTier)
                        .build())
                .build();
        final Action action = createAction(actionInfo);

        assertSame(vm, ActionDTOUtil.getPrimaryEntity(action));
        assertEquals(vm.getId(), ActionDTOUtil.getSeverityEntity(action));
        final List<ActionEntity> involvedEntities = ActionDTOUtil.getInvolvedEntities(action);
        assertEquals(ImmutableSet.of(vm, computeTier), new HashSet<>(involvedEntities));
    }

    /**
     * Test for {@link ActionDTOUtil#buildEntityName} method.
     */
    @Test
    public void testBuildEntityName() {
        final ActionEntity entity = createActionEntity(1);
        final String result = ActionDTOUtil.buildEntityName(entity);
        assertEquals("{entity:1:displayName:}", result);
    }

    /**
     * Verify that the involved entities of a (compound) Move action are computed correctly but
     * are limited to target and source.
     *
     * @throws UnsupportedActionException should not be thrown.
     */
    @Test
    public void testInvolvedEntitiesBuyRI() throws UnsupportedActionException {
        Set<Long> involvedEntities = ActionDTOUtil.getInvolvedEntityIds(
            buyRIAction, InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES);
        assertEquals(Sets.newHashSet(COMPUTE_TIER_ID, REGION_ID, MASTER_ACCOUNT_ID), involvedEntities);

        involvedEntities = ActionDTOUtil.getInvolvedEntityIds(
            buyRIAction, InvolvedEntityCalculation.INCLUDE_SOURCE_PROVIDERS_WITH_RISKS);
        assertEquals("BuyRI is not picked up by ARM", Sets.newHashSet(), involvedEntities);
    }

    /**
     * Verify that the involved entities of a (compound) Move action are computed correctly but
     * are limited to target and source.
     *
     * @throws UnsupportedActionException should not be thrown.
     */
    @Test
    public void testInvolvedEntitiesLimited() throws UnsupportedActionException {
        Set<Long> involvedEntities = ActionDTOUtil.getInvolvedEntityIds(
            action, InvolvedEntityCalculation.INCLUDE_SOURCE_PROVIDERS_WITH_RISKS);
        assertEquals(Sets.newHashSet(TARGET, SOURCE_1, SOURCE_2), involvedEntities);

        involvedEntities = ActionDTOUtil.getInvolvedEntityIds(
            scaleAction, InvolvedEntityCalculation.INCLUDE_SOURCE_PROVIDERS_WITH_RISKS);
        assertEquals(Sets.newHashSet(TARGET, SOURCE_1, SOURCE_2), involvedEntities);
    }


    /**
     * Verify that the involved entities of a (compound) Move action are computed correctly but
     * are limited to target because it's a compliance move.
     *
     * @throws UnsupportedActionException should not be thrown.
     */
    @Test
    public void testInvolvedEntitiesFromComplianceMoveLimited() throws UnsupportedActionException {
        Set<Long> involvedEntities = ActionDTOUtil.getInvolvedEntityIds(
            moveCompliance, InvolvedEntityCalculation.INCLUDE_SOURCE_PROVIDERS_WITH_RISKS);
        assertEquals(Sets.newHashSet(TARGET), involvedEntities);
    }

    /**
     * Tests that INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES does NOT include entities for
     * merged actions from the aggregated/deduplicated actions.
     *
     * @throws UnsupportedActionException should not be thrown.
     */
    @Test
    public void testInvolvedEntitiesMergedActionExcluded() throws UnsupportedActionException {
        Set<Long> involvedEntities = ActionDTOUtil.getInvolvedEntityIds(
            mergedResizeAction, InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES);
        assertEquals(Sets.newHashSet(TARGET), involvedEntities);
    }

    /**
     * Tests that INCLUDE_ALL_MERGED_INVOLVED_ENTITIES does NOT include entities for
     * merged actions from the aggregated/deduplicated actions.
     *
     * @throws UnsupportedActionException should not be thrown.
     */
    @Test
    public void testInvolvedEntitiesMergedActionIncluded() throws UnsupportedActionException {
        Set<Long> involvedEntities = ActionDTOUtil.getInvolvedEntityIds(
            mergedResizeAction, InvolvedEntityCalculation.INCLUDE_ALL_MERGED_INVOLVED_ENTITIES);
        assertEquals(Sets.newHashSet(TARGET, SOURCE_1, SOURCE_2), involvedEntities);
    }

    private static ActionEntity createActionEntity(long id) {
        // set some fake type for now
        return createActionEntity(id, 1);
    }

    private static ActionEntity createActionEntity(long id, int type) {
        return ActionEntity.newBuilder()
                .setId(id)
                .setType(type)
                .build();
    }

    private static ActionEntity createActionEntity(long id, int type, EnvironmentTypeEnum.EnvironmentType environmentType) {
        return ActionEntity.newBuilder()
                .setId(id)
                .setType(type)
                .setEnvironmentType(environmentType)
                .build();
    }

    private static Action createAction(final ActionInfo actionInfo) {
        return Action.newBuilder()
                .setId(123121)
                .setExplanation(Explanation.getDefaultInstance())
                .setInfo(actionInfo)
                .setDeprecatedImportance(1)
                .build();
    }

    private static ReasonCommodity createReasonCommodity(int baseType) {
        return ReasonCommodity.newBuilder().setCommodityType((TopologyDTO.CommodityType.newBuilder()
                        .setType(baseType).build())).build();
    }
}
