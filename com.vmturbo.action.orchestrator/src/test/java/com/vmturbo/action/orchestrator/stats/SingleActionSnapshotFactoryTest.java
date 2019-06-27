package com.vmturbo.action.orchestrator.stats;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.stats.SingleActionSnapshotFactory.SingleActionSnapshot;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup.ActionGroupKey;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
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
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class SingleActionSnapshotFactoryTest {

    @Test
    public void testFactoryFromActionView() throws UnsupportedActionException {
        // Arrange
        final SingleActionSnapshotFactory factory = new SingleActionSnapshotFactory();
        final ActionView actionView = mock(ActionView.class);
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
                                .setStartUtilization(1)
                                .setEndUtilization(2)))
                .setDeprecatedImportance(10)
                .build();
        when(actionView.getRecommendation()).thenReturn(recommendation);
        when(actionView.getState()).thenReturn(ActionState.QUEUED);
        when(actionView.getMode()).thenReturn(ActionMode.AUTOMATIC);
        when(actionView.getActionCategory()).thenReturn(ActionCategory.PERFORMANCE_ASSURANCE);

        // Act
        final SingleActionSnapshot snapshot = factory.newSnapshot(actionView);

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
