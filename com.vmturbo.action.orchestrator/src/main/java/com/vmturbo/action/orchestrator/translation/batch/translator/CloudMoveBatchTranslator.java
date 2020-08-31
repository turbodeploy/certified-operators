package com.vmturbo.action.orchestrator.translation.batch.translator;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Allocate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AllocateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ScaleExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo.ActionComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo.TypeCase;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;

/**
 * This class translates Cloud Move actions to Scale and Allocate actions.
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
        return translateCloudMoveAction(actionView.getRecommendation());
    }

    /**
     * Checks if action is a Cloud Move that needs to be translated to scale/allocate.
     *
     * @param action Action to check.
     * @return True if action is a Cloud Move that needs to be translated.
     */
    public static boolean translateCloudMoveAction(@Nonnull final ActionDTO.Action action) {
        // In case of cloud-to-cloud migration, move will be across regions, so we should
        // return false from here, as we don't want to translate such moves to scale.
        return TopologyDTOUtil.isMoveWithinSameRegion(action);
    }

    /**
     * Translates Cloud Move actions to Scale or Allocate (aka Accounting) actions.
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
            .map(action -> translate(action, snapshot))
            .collect(Collectors.toList()).stream();
    }

    private <T extends ActionView> T translate(
            @Nonnull final T action,
            @Nonnull final EntitiesAndSettingsSnapshot snapshot) {
        final ActionDTO.Action originalAction = action.getRecommendation();

        final ActionDTO.Action translatedAction = isAllocateAction(originalAction)
                ? translateToAllocate(originalAction, snapshot)
                : translateToScale(originalAction);
        action.getActionTranslation().setTranslationSuccess(translatedAction);

        return action;
    }

    private ActionDTO.Action translateToScale(@Nonnull final ActionDTO.Action actionDto) {
        final Move move = actionDto.getInfo().getMove();

        final Scale scale = Scale.newBuilder()
                .setTarget(move.getTarget())
                .addAllChanges(move.getChangesList())
                .build();

        final ScaleExplanation.Builder explanation = ScaleExplanation.newBuilder()
                .addAllChangeProviderExplanation(actionDto.getExplanation().getMove()
                        .getChangeProviderExplanationList());
        if (move.hasScalingGroupId()) {
            explanation.setScalingGroupId(move.getScalingGroupId());
        }

        return actionDto.toBuilder()
                .setExplanation(Explanation.newBuilder().setScale(explanation).build())
                .setInfo(ActionInfo.newBuilder(actionDto.getInfo()).setScale(scale).build())
                .build();
    }

    private ActionDTO.Action translateToAllocate(
            @Nonnull final ActionDTO.Action actionDto,
            @Nonnull final EntitiesAndSettingsSnapshot snapshot) {
        final Move move = actionDto.getInfo().getMove();

        Optional<ChangeProvider> changeProvider = ActionDTOUtil.getPrimaryChangeProvider(actionDto);
        if (!changeProvider.isPresent()) {
            return actionDto;
        }
        final ActionEntity workloadTier = changeProvider.get().getDestination();
        final Allocate allocate = Allocate.newBuilder()
                .setTarget(move.getTarget())
                .setWorkloadTier(workloadTier)
                .build();

        final AllocateExplanation.Builder explanation = AllocateExplanation.newBuilder();

        // Set RI instance size family
        snapshot.getEntityFromOid(workloadTier.getId())
                .map(ActionPartialEntity::getTypeSpecificInfo)
                .filter(typeInfo -> typeInfo.getTypeCase() == TypeCase.COMPUTE_TIER)
                .map(ActionEntityTypeSpecificInfo::getComputeTier)
                .map(ActionComputeTierInfo::getFamily)
                .ifPresent(explanation::setInstanceSizeFamily);

        return actionDto.toBuilder()
                .setExplanation(Explanation.newBuilder()
                        .setAllocate(explanation).build())
                .setInfo(ActionInfo.newBuilder(actionDto.getInfo())
                        .setAllocate(allocate).build())
                .build();
    }

    private static boolean isAllocateAction(final ActionDTO.Action actionDto) {
        final ChangeProvider changeProvider = ActionDTOUtil.getPrimaryChangeProvider(actionDto).orElse(null);
        return changeProvider != null && changeProvider.hasSource() && changeProvider.hasDestination()
                && changeProvider.getSource().getId() == changeProvider.getDestination().getId();
    }
}
