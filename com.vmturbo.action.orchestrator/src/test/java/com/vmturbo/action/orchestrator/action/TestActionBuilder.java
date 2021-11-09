package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils.createActionEntity;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfoOrBuilder;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class TestActionBuilder {

    private final AtomicLong actionId = new AtomicLong();

    @Nonnull
    public Action buildMoveAction(long targetId,
                                  long sourceId,
                                  int sourceType,
                                  long destinationId,
                                  int destinationType) {
        return buildMoveAction(targetId, sourceId, sourceType, destinationId, destinationType, null);
    }

    @Nonnull
    public Action buildMoveAction(long targetId,
                                  long sourceId,
                                  int sourceType,
                                  long destinationId,
                                  int destinationType,
                                  @Nullable String scalingGroupId) {
        return Action.newBuilder().setId(actionId.getAndIncrement()).setDeprecatedImportance(1)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(makeMoveInfo(targetId, sourceId, sourceType, destinationId, destinationType, scalingGroupId))
                .setSupportingLevel(SupportLevel.SUPPORTED)
                .build();
    }

    public static ActionInfo.Builder makeMoveInfo(
            long targetId,
            long sourceId,
            int sourceType,
            long destinationId,
            int destinationType) {
        return makeMoveInfo(targetId, sourceId, sourceType, destinationId, destinationType, null);
    }

    public static ActionInfo.Builder makeMoveInfo(
            long targetId,
            long sourceId,
            int sourceType,
            long destinationId,
            int destinationType,
            @Nullable String scalingGroupId) {

        Move.Builder moveBuilder = Move.newBuilder()
                .setTarget(createActionEntity(targetId))
                .addChanges(ChangeProvider.newBuilder()
                        .setSource(ActionEntity.newBuilder()
                                .setId(sourceId)
                                .setType(sourceType)
                                .build())
                        .setDestination(ActionEntity.newBuilder()
                                .setId(destinationId)
                                .setType(destinationType)
                                .build())
                        .build());
        if (scalingGroupId != null) {
            moveBuilder.setScalingGroupId(scalingGroupId);
        }
        return ActionInfo.newBuilder().setMove(moveBuilder.build());
    }

    /**
     * Construct a container atomic resize action info given the followings:
     *
     * @param workloadControllerId id of the workload controller, the execution target
     * @param containerSpecId id of the one single container spec
     * @param containerIds ids of the containers
     * @return the construct atomic resize action info
     */
    @Nonnull
    public static ActionInfo.Builder makeAtomicResizeInfo(long workloadControllerId,
            long containerSpecId, Collection<Long> containerIds) {
        final AtomicResize.Builder atomicResizeBuilder = AtomicResize.newBuilder()
                .setExecutionTarget(createActionEntity(workloadControllerId, EntityType.WORKLOAD_CONTROLLER_VALUE))
                .addResizes(ResizeInfo.newBuilder()
                        .setTarget(createActionEntity(containerSpecId, EntityType.CONTAINER_SPEC_VALUE))
                        .addAllSourceEntities(containerIds.stream()
                                .map(id -> createActionEntity(id, EntityType.CONTAINER_VALUE))
                                .collect(Collectors.toList())));
        return ActionInfo.newBuilder().setAtomicResize(atomicResizeBuilder);
    }

    @Nonnull
    public Action buildProvisionAction(long entityToCloneId,
                                       @Nullable Long provisionedSeller) {

        return Action.newBuilder().setId(actionId.getAndIncrement()).setDeprecatedImportance(1)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(makeProvisionInfo(entityToCloneId, provisionedSeller))
                .build();
    }

    public static ActionInfo.Builder makeProvisionInfo(long entityToCloneId,
                                                       @Nullable Long provisionedSeller) {

        Provision.Builder provisionOrBuilder = Provision.newBuilder()
                .setEntityToClone(createActionEntity(entityToCloneId));
        if (provisionedSeller != null) {
            provisionOrBuilder.setProvisionedSeller(provisionedSeller);
        }
        return ActionInfo.newBuilder().setProvision(provisionOrBuilder);

    }

    /**
     * Build scale action.
     *
     * @param targetId target entity id.
     * @param sourceId scale action source id.
     * @param sourceType scale action source type.
     * @param destinationId scale action destination id.
     * @param destinationType scale action destination type.
     * @param scalingGroupId scalingGroupId related to the target entity.
     * @return Scale action object.
     */
    @Nonnull
    public Action buildScaleAction(long targetId, long sourceId, int sourceType, long destinationId,
                                   int destinationType, @Nullable String scalingGroupId) {
        return Action.newBuilder().setId(actionId.getAndIncrement()).setDeprecatedImportance(1)
                .setExplanation(Explanation.newBuilder().build())
                .setInfo(makeScaleInfo(targetId, sourceId, sourceType, destinationId, destinationType, scalingGroupId))
                .setSupportingLevel(SupportLevel.SUPPORTED)
                .build();
    }

    private static ActionInfo.Builder makeScaleInfo(long targetId, long sourceId, int sourceType,
                                                    long destinationId, int destinationType, @Nullable String scalingGroupId) {

        Scale.Builder scaleBuilder = Scale.newBuilder()
                .setTarget(createActionEntity(targetId, EntityType.VIRTUAL_VOLUME_VALUE))
                .addChanges(ChangeProvider.newBuilder()
                        .setSource(ActionEntity.newBuilder()
                                .setId(sourceId)
                                .setType(sourceType)
                                .build())
                        .setDestination(ActionEntity.newBuilder()
                                .setId(destinationId)
                                .setType(destinationType)
                                .build())
                        .build());
        if (scalingGroupId != null) {
            scaleBuilder.setScalingGroupId(scalingGroupId);
        }
        return ActionInfo.newBuilder().setScale(scaleBuilder.build());
    }
}
