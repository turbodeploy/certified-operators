package com.vmturbo.common.protobuf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Congestion;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Efficiency;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.InitialPlacement;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation.CommodityMaxAmountAvailableEntry;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.TopologyDTO;


/**
 * Unit tests for {@link ActionDTOUtil}.
 */
public class ActionDTOUtilTest {

    private static final long TARGET = 10L;

    private static final long SOURCE_1 = 100L;

    private static final long DEST_1 = 101L;

    private static final long SOURCE_2 = 200L;

    private static final long DEST_2 = 201L;

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
    public void testMapNormalSeverity() {
        assertEquals(Severity.NORMAL,
            ActionDTOUtil.mapActionCategoryToSeverity(ActionCategory.UNKNOWN));
    }

    @Test
    public void testMapMinorSeverity() {
        assertEquals(Severity.MINOR,
            ActionDTOUtil.mapActionCategoryToSeverity(ActionCategory.EFFICIENCY_IMPROVEMENT));
    }

    @Test
    public void testMapMajorSeverity() {
        assertEquals(Severity.MAJOR,
            ActionDTOUtil.mapActionCategoryToSeverity(ActionCategory.PREVENTION));
    }

    @Test
    public void testMapCriticalSeverity() {
        assertEquals(Severity.CRITICAL,
            ActionDTOUtil.mapActionCategoryToSeverity(ActionCategory.PERFORMANCE_ASSURANCE));
        assertEquals(Severity.CRITICAL,
            ActionDTOUtil.mapActionCategoryToSeverity(ActionCategory.COMPLIANCE));
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
                            .addCongestedCommodities(commodity1)
                            .addUnderUtilizedCommodities(commodity2)))))
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

    private static ActionEntity createActionEntity(long id) {
        // set some fake type for now
        final int defaultEntityType = 1;
        return ActionEntity.newBuilder()
            .setId(id)
            .setType(defaultEntityType)
            .build();
    }

    private static ReasonCommodity createReasonCommodity(int baseType) {
        return ReasonCommodity.newBuilder().setCommodityType((TopologyDTO.CommodityType.newBuilder()
                        .setType(baseType).build())).build();
    }
}

