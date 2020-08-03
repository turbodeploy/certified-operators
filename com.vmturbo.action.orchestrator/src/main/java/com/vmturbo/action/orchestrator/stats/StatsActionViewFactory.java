package com.vmturbo.action.orchestrator.stats;

import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup.ActionGroupKey;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableActionGroupKey;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;

/**
 * A factory class for {@link StatsActionView}s.
 */
public class StatsActionViewFactory {

    /**
     * Snapshot an {@link ActionView} and return the snapshot.
     *
     * @param action The input {@link ActionView}.
     * @return A {@link StatsActionView} representing the state of the {@link ActionView} at
     *         the time the factory method was called. Further modifications to the underlying
     *         action (e.g. accepting a READY action) won't be reflected in the snapshot.
     * @throws UnsupportedActionException If the recommendation of the {@link ActionView} is not
     *         supported. This shouldn't happen, unless there is some coding error.
     */
    @Nonnull
    public StatsActionView newStatsActionView(@Nonnull final ActionView action)
        throws UnsupportedActionException {
        final ImmutableStatsActionView.Builder snapshotBuilder =
            ImmutableStatsActionView.builder()
                .recommendation(action.getTranslationResultOrOriginal())
                .actionGroupKey(ImmutableActionGroupKey.builder()
                    .actionType(ActionDTOUtil.getActionInfoActionType(
                            action.getTranslationResultOrOriginal()))
                    .actionState(action.getState())
                    .actionMode(action.getMode())
                    .category(action.getActionCategory())
                    .build());
        // only include the target entity in the numEntities for action stats, because that is the
        // entity impacted by this action, which also aligns with classic.
        // for example: a move vm action may involve target entity (vm), current entity (current
        // host), new entity (new host), we should only count the vm in "numEntities" stats.
        snapshotBuilder.addInvolvedEntities(ActionDTOUtil.getPrimaryEntity(
                action.getTranslationResultOrOriginal()));
        return snapshotBuilder.build();
    }

    /**
     * The snapshot of a single {@link ActionView} at a particular point in time.
     */
    @Value.Immutable
    public interface StatsActionView {
        ActionDTO.Action recommendation();
        ActionGroupKey actionGroupKey();
        Set<ActionEntity> involvedEntities();
    }
}
