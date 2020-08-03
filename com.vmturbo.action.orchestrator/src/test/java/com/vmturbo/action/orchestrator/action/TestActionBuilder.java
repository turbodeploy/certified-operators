package com.vmturbo.action.orchestrator.action;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.ActionTest;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfoOrBuilder;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision.Builder;
import com.vmturbo.common.protobuf.action.ActionDTO.ProvisionOrBuilder;

public class TestActionBuilder {

    private final AtomicLong actionId = new AtomicLong();

    @Nonnull
    public Action buildMoveAction(long targetId,
                                  long sourceId,
                                  int sourceType,
                                  long destinationId,
                                  int destinationType) {
        return Action.newBuilder().setId(actionId.getAndIncrement()).setDeprecatedImportance(1)
            .setExplanation(Explanation.newBuilder().build())
            .setInfo(makeMoveInfo(targetId, sourceId, sourceType, destinationId, destinationType))
            .setSupportingLevel(SupportLevel.SUPPORTED)
            .build();
    }

    public static ActionInfo.Builder makeMoveInfo(
            long targetId,
            long sourceId,
            int sourceType,
            long destinationId,
            int destinationType) {

        return ActionInfo.newBuilder().setMove(Move.newBuilder()
                .setTarget(ActionOrchestratorTestUtils.createActionEntity(targetId))
                .addChanges(ChangeProvider.newBuilder()
                        .setSource(ActionEntity.newBuilder()
                                .setId(sourceId)
                                .setType(sourceType)
                                .build())
                        .setDestination(ActionEntity.newBuilder()
                                .setId(destinationId)
                                .setType(destinationType)
                                .build())
                        .build())
                .build());
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

        Builder provisionOrBuilder = Provision.newBuilder()
                .setEntityToClone(ActionOrchestratorTestUtils.createActionEntity(entityToCloneId));
        if (provisionedSeller != null) {
            provisionOrBuilder.setProvisionedSeller(provisionedSeller);
        }
        return ActionInfo.newBuilder().setProvision(provisionOrBuilder);

    }
}
