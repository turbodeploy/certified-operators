package com.vmturbo.action.orchestrator.stats;

import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.immutables.value.Value;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.ExplanationComposer;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup.ActionGroupKey;
import com.vmturbo.action.orchestrator.stats.groups.ImmutableActionGroupKey;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;

/**
 * A factory class for {@link StatsActionView}s.
 */
public class StatsActionViewFactory {

    /**
     * Get the snapshots for an {@link ActionView}, which can be multiple if the action has
     * multiple risks.
     *
     * @param action The input {@link ActionView}.
     * @return A stream of {@link StatsActionView} representing the state of the {@link ActionView}
     *         at the time the factory method was called. Further modifications to the underlying
     *         action (e.g. accepting a READY action) won't be reflected in the snapshot.
     * @throws UnsupportedActionException If the recommendation of the {@link ActionView} is not
     *         supported. This shouldn't happen, unless there is some coding error.
     */
    @Nonnull
    public Stream<StatsActionView> newStatsActionView(@Nonnull final ActionView action)
            throws UnsupportedActionException {
        final Action translationResultOrOriginal = action.getTranslationResultOrOriginal();
        final ActionType actionType = ActionDTOUtil.getActionInfoActionType(translationResultOrOriginal);
        // only include the target entity in the numEntities for action stats, because that is the
        // entity impacted by this action, which also aligns with classic.
        // for example: a move vm action may involve target entity (vm), current entity (current
        // host), new entity (new host), we should only count the vm in "numEntities" stats.
        final ActionEntity primaryEntity = ActionDTOUtil.getPrimaryEntity(translationResultOrOriginal);
        return ExplanationComposer.composeRelatedRisks(translationResultOrOriginal).stream().map(relatedRisk -> {
            final ImmutableStatsActionView.Builder snapshotBuilder = ImmutableStatsActionView.builder()
                    .recommendation(translationResultOrOriginal)
                    .actionGroupKey(ImmutableActionGroupKey.builder()
                            .actionType(actionType)
                            .actionState(action.getState())
                            .actionMode(action.getMode())
                            .category(action.getActionCategory())
                            .actionRelatedRisk(relatedRisk)
                            .build());
            snapshotBuilder.addInvolvedEntities(primaryEntity);
            return snapshotBuilder.build();
        });
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
