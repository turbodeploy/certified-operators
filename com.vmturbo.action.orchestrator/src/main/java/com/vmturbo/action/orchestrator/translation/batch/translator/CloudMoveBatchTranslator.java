package com.vmturbo.action.orchestrator.translation.batch.translator;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ScaleExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;

/**
 * This class translates Cloud Move actions to Scale actions.
 */
public class CloudMoveBatchTranslator implements BatchTranslator {

    /**
     * Checks if {@code CloudMoveBatchTranslator} should be applied to the action.
     * Implementation returns {@code true} for Move actions where both source and destination are
     * Cloud tiers for workloads (VM, DB, DB Server).
     *
     * @param actionView Action to check.
     * @return True if {@code CloudMoveBatchTranslator} should be applied to the action.
     */
    @Override
    public boolean appliesTo(@Nonnull final ActionView actionView) {
        return isCloudMoveAction(actionView.getRecommendation());
    }

    /**
     * Checks if action is a Cloud Move.
     *
     * @param action Action to check.
     * @return True if action is a Cloud Move.
     */
    public static boolean isCloudMoveAction(@Nonnull final ActionDTO.Action action) {
        final ActionInfo actionInfo = action.getInfo();
        return actionInfo.getActionTypeCase() == ActionTypeCase.MOVE
                && actionInfo.getMove().getChangesList().stream()
                .anyMatch(m -> m.hasSource()
                        && TopologyDTOUtil.isPrimaryTierEntityType(m.getSource().getType())
                        && TopologyDTOUtil.isPrimaryTierEntityType(m.getDestination().getType()));
    }

    /**
     * Translates Cloud Move actions to Scale actions.
     *
     * @param moveActions Original Move actions.
     * @param snapshot A snapshot of all the entities and settings involved in the actions.
     * @param <T> Action class type.
     * @return A stream of converted Scale actions.
     */
    @Override
    public <T extends ActionView> Stream<T> translate(
            @Nonnull final List<T> moveActions,
            @Nonnull final EntitiesAndSettingsSnapshot snapshot) {
        return moveActions.stream()
            .map(this::translate)
            .collect(Collectors.toList()).stream();
    }

    private <T extends ActionView> T translate(@Nonnull final T action) {
        final ActionDTO.Action actionDto = action.getRecommendation();

        final Move move = actionDto.getInfo().getMove();
        final Scale scale = Scale.newBuilder()
            .setTarget(move.getTarget())
            .addAllChanges(move.getChangesList())
            .build();

        final ScaleExplanation.Builder explanation = ScaleExplanation.newBuilder()
            .addAllChangeProviderExplanation(actionDto.getExplanation().getMove()
                .getChangeProviderExplanationList());

        action.getActionTranslation().setTranslationSuccess(actionDto.toBuilder()
            .setExplanation(Explanation.newBuilder().setScale(explanation).build())
            .setInfo(ActionInfo.newBuilder(actionDto.getInfo()).setScale(scale).build())
            .build());

        return action;
    }
}
