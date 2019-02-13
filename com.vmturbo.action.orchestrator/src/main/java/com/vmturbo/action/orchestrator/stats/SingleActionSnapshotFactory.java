package com.vmturbo.action.orchestrator.stats;

import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup.ActionGroupKey;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableActionGroupKey;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;

/**
 * A factory class for {@link SingleActionSnapshot}s.
 */
public class SingleActionSnapshotFactory {

    /**
     * Snapshot an {@link ActionView} and return the snapshot.
     *
     * @param action The input {@link ActionView}.
     * @return A {@link SingleActionSnapshot} representing the state of the {@link ActionView} at
     *         the time the factory method was called. Further modifications to the underlying
     *         action (e.g. accepting a READY action) won't be reflected in the snapshot.
     * @throws UnsupportedActionException If the recommendation of the {@link ActionView} is not
     *         supported. This shouldn't happen, unless there is some coding error.
     */
    @Nonnull
    public SingleActionSnapshot newSnapshot(@Nonnull final ActionView action) throws UnsupportedActionException {
        final ImmutableSingleActionSnapshot.Builder snapshotBuilder =
            ImmutableSingleActionSnapshot.builder()
                .recommendation(action.getRecommendation())
                .actionGroupKey(ImmutableActionGroupKey.builder()
                    .actionType(ActionDTOUtil.getActionInfoActionType(action.getRecommendation()))
                    .actionState(action.getState())
                    .actionMode(action.getMode())
                    .category(action.getActionCategory())
                    .build());
        ActionDTOUtil.getInvolvedEntities(action.getRecommendation())
            .forEach(snapshotBuilder::addInvolvedEntities);
        return snapshotBuilder.build();
    }

    /**
     * The snapshot of a single {@link ActionView} at a particular point in time.
     */
    @Value.Immutable
    public interface SingleActionSnapshot {
        ActionDTO.Action recommendation();
        ActionGroupKey actionGroupKey();
        Set<ActionEntity> involvedEntities();
    }
}
