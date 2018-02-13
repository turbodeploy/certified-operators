package com.vmturbo.components.test.utilities.utils;

import static com.vmturbo.components.test.utilities.utils.ActionPlanGenerator.activateCount;
import static com.vmturbo.components.test.utilities.utils.ActionPlanGenerator.deactivateCount;
import static com.vmturbo.components.test.utilities.utils.ActionPlanGenerator.moveCount;
import static com.vmturbo.components.test.utilities.utils.ActionPlanGenerator.provisionCount;
import static com.vmturbo.components.test.utilities.utils.ActionPlanGenerator.reconfigureCount;
import static com.vmturbo.components.test.utilities.utils.ActionPlanGenerator.resizeCount;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.test.utilities.utils.ActionPlanGenerator.ActivateCount;
import com.vmturbo.components.test.utilities.utils.ActionPlanGenerator.DeactivateCount;
import com.vmturbo.components.test.utilities.utils.ActionPlanGenerator.MoveCount;
import com.vmturbo.components.test.utilities.utils.ActionPlanGenerator.ProvisionCount;
import com.vmturbo.components.test.utilities.utils.ActionPlanGenerator.ReconfigureCount;
import com.vmturbo.components.test.utilities.utils.ActionPlanGenerator.ResizeCount;

public class ActionPlanGeneratorTest {

    private final ActionPlanGenerator generator = new ActionPlanGenerator();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
    }

    @Test
    public void testGenerateIllegalActionCount() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        generator.generate(-1, 1, 2);
    }

    @Test
    public void testGenerateNoActions() throws Exception {
        final ActionPlan plan = generator.generate(0, 1, 2);
        assertEquals(0, plan.getActionCount());
    }

    @Test
    public void testGenerateOneAction() throws Exception {
        final ActionPlan plan = generator.generate(1, 1, 2);
        assertEquals(1, plan.getActionCount());

        // The action generated should be a move.
        assertEquals(1, plan.getTopologyId());
        assertEquals(2, plan.getTopologyContextId());
        assertEquals(ActionTypeCase.MOVE, plan.getAction(0).getInfo().getActionTypeCase());
    }

    @Test
    public void testGeneratePicksOidsFromTopology() {
        List<Long> oids = LongStream.range(0, 10)
            .mapToObj(l -> l)
            .collect(Collectors.toList());
        final ActionPlan plan = generator.generate(30, 1, 2, oids);

        plan.getActionList().stream()
            .filter(action -> action.getInfo().getActionTypeCase() == ActionTypeCase.MOVE)
            .map(action -> action.getInfo().getMove())
            .forEach(move -> {
             // TODO(COMPOUND): add compound moves with more than 1 change
                assertEquals(1, move.getChangesCount());
                assertThat(oids, hasItem(move.getChanges(0).getSourceId()));
                assertThat(oids, hasItem(move.getChanges(0).getDestinationId()));
                assertThat(oids, hasItem(move.getTargetId()));
            });
    }

    @Test
    public void testGenerateSpecificTypesOfActions() {
        List<Long> oids = LongStream.range(0, 10)
            .mapToObj(l -> l)
            .collect(Collectors.toList());
        final ActionPlan plan = generator.generate(1, 2, oids,
            reconfigureCount(2), moveCount(10), activateCount(1),
            resizeCount(3), provisionCount(6), deactivateCount(2));

        Map<ActionTypeCase, Long> actionCounts = plan.getActionList().stream()
            .map(Action::getInfo)
            .collect(Collectors.groupingBy(ActionInfo::getActionTypeCase, Collectors.counting()));

        assertEquals(10L, (long)actionCounts.get(ActionTypeCase.MOVE));
        assertEquals(3L, (long)actionCounts.get(ActionTypeCase.RESIZE));
        assertEquals(6L, (long)actionCounts.get(ActionTypeCase.PROVISION));
        assertEquals(2L, (long)actionCounts.get(ActionTypeCase.DEACTIVATE));
        assertEquals(1L, (long)actionCounts.get(ActionTypeCase.ACTIVATE));
        assertEquals(2L, (long)actionCounts.get(ActionTypeCase.RECONFIGURE));
    }

    @Test
    public void testActionTypeCountDisallowsNegative() {
        expectedException.expect(IllegalArgumentException.class);
        moveCount(-1);
    }

    @Test
    public void testMoveCount() {
        assertThat(moveCount(1), instanceOf(MoveCount.class));
    }

    @Test
    public void testReconfigureCount() {
        assertThat(reconfigureCount(1), instanceOf(ReconfigureCount.class));
    }

    @Test
    public void testProvisionCount() {
        assertThat(provisionCount(1), instanceOf(ProvisionCount.class));
    }

    @Test
    public void testResizeCount() {
        assertThat(resizeCount(1), instanceOf(ResizeCount.class));
    }

    @Test
    public void testActivateCount() {
        assertThat(activateCount(1), instanceOf(ActivateCount.class));
    }

    @Test
    public void testDeactivateCount() {
        assertThat(deactivateCount(1), instanceOf(DeactivateCount.class));
    }

    /**
     * Generating two action plans with the same generator and identical inputs should
     * result in the same recommendations.
     *
     * We can't compare the plans directly because ids may be different. But the contents
     * of the actions (info) should all be the same.
     */
    @Test
    public void testDeterminismSameGenerator() {
        final List<ActionInfo> firstRecommendations = getRecommendations(generator.generate(500, 1, 2));
        final List<ActionInfo> secondRecommendations = getRecommendations(generator.generate(500, 1, 2));

        assertEquals(firstRecommendations, secondRecommendations);
    }

    /**
     * Generating two action plans with the different generators and the same seed using
     * identical inputs should result in the same recommendations.
     */
    @Test
    public void testDeterminismSameSeed() {
        final ActionPlanGenerator firstGen = new ActionPlanGenerator(4317);
        final ActionPlanGenerator secondGen = new ActionPlanGenerator(4317);

        final List<ActionInfo> firstRecommendations = getRecommendations(firstGen.generate(350, 1, 2));
        final List<ActionInfo> secondRecommendations = getRecommendations(secondGen.generate(350, 1, 2));

        assertEquals(firstRecommendations, secondRecommendations);
    }

    private List<ActionInfo> getRecommendations(@Nonnull final ActionPlan actionPlan) {
        return actionPlan.getActionList().stream()
            .map(Action::getInfo)
            .collect(Collectors.toList());
    }
}