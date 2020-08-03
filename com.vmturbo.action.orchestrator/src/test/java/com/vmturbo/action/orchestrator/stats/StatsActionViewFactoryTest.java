package com.vmturbo.action.orchestrator.stats;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.stats.StatsActionViewFactory.StatsActionView;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup.ActionGroupKey;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class StatsActionViewFactoryTest {

    @Test
    public void testFactoryFromActionView() throws UnsupportedActionException {
        // Arrange
        final StatsActionViewFactory factory = new StatsActionViewFactory();
        final ActionEntity targetEntity = ActionEntity.newBuilder()
                .setId(1L)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();
        final ActionDTO.Action recommendation = ActionDTO.Action.newBuilder()
                .setId(7L)
                .setInfo(ActionInfo.newBuilder()
                        .setResize(Resize.newBuilder()
                                .setTarget(targetEntity)))
                .setExplanation(Explanation.newBuilder()
                        .setResize(ResizeExplanation.newBuilder()
                                .setDeprecatedStartUtilization(1)
                                .setDeprecatedEndUtilization(2)))
                .setDeprecatedImportance(10)
                .build();
        final ActionView actionView = ActionOrchestratorTestUtils.mockActionView(recommendation);
        when(actionView.getState()).thenReturn(ActionState.QUEUED);
        when(actionView.getMode()).thenReturn(ActionMode.AUTOMATIC);
        when(actionView.getActionCategory()).thenReturn(ActionCategory.PERFORMANCE_ASSURANCE);

        // Act
        final StatsActionView snapshot = factory.newStatsActionView(actionView);

        // Assert
        assertThat(snapshot.involvedEntities(), contains(targetEntity));
        assertThat(snapshot.recommendation(), is(recommendation));

        final ActionGroupKey actionGroupKey = snapshot.actionGroupKey();
        assertThat(actionGroupKey.getActionMode(), is(ActionMode.AUTOMATIC));
        assertThat(actionGroupKey.getActionState(), is(ActionState.QUEUED));
        assertThat(actionGroupKey.getActionType(), is(ActionType.RESIZE));
        assertThat(actionGroupKey.getCategory(), is(ActionCategory.PERFORMANCE_ASSURANCE));
    }
}
