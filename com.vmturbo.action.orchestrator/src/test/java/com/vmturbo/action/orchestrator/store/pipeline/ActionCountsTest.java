package com.vmturbo.action.orchestrator.store.pipeline;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests for {@link ActionCounts}.
 */
public class ActionCountsTest {
    private static final long ACTION_PLAN_ID = 123456;

    /**
     * setup.
     */
    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
    }

    /**
     * Test rendering action counts to string.
     */
    @Test
    public void testToString() {
        final List<Action> actions = Arrays.asList(
            ActionOrchestratorTestUtils.createMoveRecommendation(1),
            ActionOrchestratorTestUtils.createMoveRecommendation(2),
            ActionOrchestratorTestUtils.createMoveRecommendation(3),
            ActionOrchestratorTestUtils.createMoveRecommendation(4),

            ActionOrchestratorTestUtils.createProvisionRecommendation(100, 0, EntityType.PHYSICAL_MACHINE_VALUE),
            ActionOrchestratorTestUtils.createProvisionRecommendation(200, 0, EntityType.CONTAINER_POD_VALUE),

            createResizeRecommendation(10, EntityType.VIRTUAL_MACHINE_VALUE),
            createResizeRecommendation(20, EntityType.CONTAINER_VALUE),
            createResizeRecommendation(30, EntityType.CONTAINER_VALUE));

        final String str = new ActionCounts("Test Action Counts", actions.stream()).toString();
        assertThat(str, containsString("Test Action Counts"));
        assertThat(str, containsString("TOTAL: 9"));
        assertThat(str, containsString("RESIZE: 3"));
        assertThat(str, containsString("PROVISION: 2"));
        assertThat(str, containsString("MOVE: 4"));

        assertThat(str, containsString("VIRTUAL_MACHINE: 4"));
        assertThat(str, containsString("VIRTUAL_MACHINE: 1"));
        assertThat(str, containsString("CONTAINER: 2"));
        assertThat(str, containsString("CONTAINER_POD: 1"));
        assertThat(str, containsString("PHYSICAL_MACHINE: 1"));
    }

    /**
     * Test that the size in bytes is rendered for ActionDTO recommendations.
     */
    @Test
    public void testSizeRenderedForRecommendations() {
        final List<Action> actions = Arrays.asList(
            ActionOrchestratorTestUtils.createMoveRecommendation(1),
            ActionOrchestratorTestUtils.createMoveRecommendation(2));

        final String str = new ActionCounts("Test Action Counts", actions.stream()).toString();
        assertThat(str, containsString("Bytes"));
    }

    /**
     * Test that size in bytes is not recommended for action model objects.
     */
    @Test
    public void testSizeNotRenderedForActionModel() {
        final List<com.vmturbo.action.orchestrator.action.Action> actions = Arrays.asList(
            ActionOrchestratorTestUtils.createMoveAction(1, ACTION_PLAN_ID),
            ActionOrchestratorTestUtils.createMoveAction(2, ACTION_PLAN_ID));

        final String str = new ActionCounts("Test Action Counts", actions).toString();
        assertThat(str, not(containsString("Bytes")));
    }

    /**
     * Test that the difference between two action counts is rendered correctly.
     */
    @Test
    public void testDifference() {
        final List<Action> before = Arrays.asList(
            ActionOrchestratorTestUtils.createProvisionRecommendation(100, 0, EntityType.PHYSICAL_MACHINE_VALUE),

            createResizeRecommendation(10, EntityType.VIRTUAL_MACHINE_VALUE),
            createResizeRecommendation(20, EntityType.CONTAINER_VALUE),
            createResizeRecommendation(30, EntityType.CONTAINER_VALUE));

        final List<Action> after = Arrays.asList(
            ActionOrchestratorTestUtils.createMoveRecommendation(1),
            ActionOrchestratorTestUtils.createMoveRecommendation(2),
            ActionOrchestratorTestUtils.createMoveRecommendation(3),
            ActionOrchestratorTestUtils.createMoveRecommendation(4),

            ActionOrchestratorTestUtils.createProvisionRecommendation(100, 0, EntityType.PHYSICAL_MACHINE_VALUE),
            ActionOrchestratorTestUtils.createProvisionRecommendation(100, 0, EntityType.CONTAINER_POD_VALUE),

            createResizeRecommendation(20, EntityType.CONTAINER_VALUE)
        );

        final ActionCounts beforeCounts = new ActionCounts("Before", before.stream());
        final ActionCounts afterCounts = new ActionCounts("After", after.stream());

        final String difference = afterCounts.difference(beforeCounts);
        System.out.println(difference);

        assertThat(difference, stringContainsInOrder("TOTAL: 7", "(+3)"));
        assertThat(difference, stringContainsInOrder("MOVE: 4", "(+4)"));
        assertThat(difference, stringContainsInOrder("PROVISION: 2", "(+1)"));
        assertThat(difference, stringContainsInOrder("RESIZE: 1", "(-2)"));
        assertThat(difference, stringContainsInOrder("VIRTUAL_MACHINE: 4", "(+4)"));
        assertThat(difference, stringContainsInOrder("CONTAINER_POD: 1", "(+1)"));
        assertThat(difference, stringContainsInOrder("PHYSICAL_MACHINE: 1", "(0)"));
        assertThat(difference, stringContainsInOrder("CONTAINER: 1", "(-1)"));
        assertThat(difference, stringContainsInOrder("CONTAINER: 1", "(-1)"));
        assertThat(difference, stringContainsInOrder("VIRTUAL_MACHINE: 0", "(-1)"));
    }

    /**
     * Test that the difference string when all actions currently present are removed.
     */
    @Test
    public void testDifferenceAllActionsGone() {
        final List<Action> before = Arrays.asList(
            ActionOrchestratorTestUtils.createProvisionRecommendation(100, 0, EntityType.PHYSICAL_MACHINE_VALUE),

            createResizeRecommendation(10, EntityType.VIRTUAL_MACHINE_VALUE),
            createResizeRecommendation(20, EntityType.CONTAINER_VALUE),
            createResizeRecommendation(30, EntityType.CONTAINER_VALUE));

        final List<Action> after = Collections.emptyList();

        final ActionCounts beforeCounts = new ActionCounts("Before", before.stream());
        final ActionCounts afterCounts = new ActionCounts("After", after.stream());

        final String difference = afterCounts.difference(beforeCounts);
        System.out.println(difference);

        assertThat(difference, stringContainsInOrder("TOTAL: 0", "(-4)"));
        assertThat(difference, stringContainsInOrder("PROVISION: 0", "(-1)"));
        assertThat(difference, stringContainsInOrder("PHYSICAL_MACHINE: 0", "(-1)"));
        assertThat(difference, stringContainsInOrder("RESIZE: 0", "(-3)"));
        assertThat(difference, stringContainsInOrder("VIRTUAL_MACHINE: 0", "(-1)"));
        assertThat(difference, stringContainsInOrder("CONTAINER: 0", "(-2)"));
    }

    /**
     * Test rendering the difference when all actions are new.
     */
    @Test
    public void testDifferenceAllActionsNew() {
        final List<Action> before = Collections.emptyList();
        final List<Action> after = Arrays.asList(
            ActionOrchestratorTestUtils.createProvisionRecommendation(100, 0, EntityType.PHYSICAL_MACHINE_VALUE),

            createResizeRecommendation(10, EntityType.VIRTUAL_MACHINE_VALUE),
            createResizeRecommendation(20, EntityType.CONTAINER_VALUE),
            createResizeRecommendation(30, EntityType.CONTAINER_VALUE));

        final ActionCounts beforeCounts = new ActionCounts("Before", before.stream());
        final ActionCounts afterCounts = new ActionCounts("After", after.stream());

        final String difference = afterCounts.difference(beforeCounts);
        System.out.println(difference);

        assertThat(difference, stringContainsInOrder("TOTAL: 4", "(+4)"));
        assertThat(difference, stringContainsInOrder("PROVISION: 1", "(+1)"));
        assertThat(difference, stringContainsInOrder("PHYSICAL_MACHINE: 1", "(+1)"));
        assertThat(difference, stringContainsInOrder("RESIZE: 3", "(+3)"));
        assertThat(difference, stringContainsInOrder("VIRTUAL_MACHINE: 1", "(+1)"));
        assertThat(difference, stringContainsInOrder("CONTAINER: 2", "(+2)"));
    }

    private static ActionDTO.Action createResizeRecommendation(long actionId, int entityType) {
        return ActionOrchestratorTestUtils.createResizeRecommendation(actionId,
            1, 1, 1, 1, entityType);
    }
}
