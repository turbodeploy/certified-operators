package com.vmturbo.topology.processor.controllable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.AffectedEntitiesDTO.ActionEffectType;
import com.vmturbo.common.protobuf.action.AffectedEntitiesDTO.AffectedEntitiesTimeoutConfig;
import com.vmturbo.common.protobuf.action.AffectedEntitiesDTO.GetAffectedEntitiesRequest;
import com.vmturbo.common.protobuf.action.AffectedEntitiesServiceGrpc.AffectedEntitiesServiceBlockingStub;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;

/**
 * Re-implmenets EntityActionDao using ActionOrchestrator's AffectedEntitiesService as the backend
 * instead of TP's database. AO already stores action execution information so there's no need
 * to duplicate the database effort.
 *
 * <p>Secondly, TP doesn't understand action steps (PRE/REPLACE/POST) come together.
 * Only Action Orchestrators understands this.</p>
 *
 * <p>Finally, Topology processor does to much, one less thing will benefit it.</p>
 */
public class AffectedEntitiesServiceEntityActionDao implements EntityActionDao {

    private final AffectedEntitiesServiceBlockingStub affectedEntitiesService;

    // For "succeed" move action records, if their last update time is older than this time threshold,
    // they will be deleted from entity action table. This makes succeed entities will not participate
    // Market analysis immediately, it will have some default cool down time.
    private final long moveSucceedRecordExpiredMsec;

    // For "in progress" or 'queued' action records. if their last update time is older than this
    // time threshold, they will be deleted from entity action tables. This try to handle the case that
    // if Probe is down and can not send back action progress notification, they will be considered
    // as time out actions and be cleaned up.
    private final long inProgressActionExpiredMsec;

    // For "succeed" activate action records, if their last update time is older than this time threshold,
    // they will be deleted from entity action table. This immediately allow succeed entities suspendable.
    private final long activateSucceedExpiredMsec;

    // For "succeed" scale (on cloud) action records, if their last update time is older than this time threshold,
    // they will be deleted from entity action table.
    private final long scaleSucceedRecordExpiredMsec;

    // For "succeed" resize (on prem) action records, if their last update time is older than this time threshold,
    // they will be deleted from entity action table.
    private final long resizeSucceedRecordExpiredMsec;

    /**
     * Creates an instance of the EntityActionDao with the provided timeout configs.
     *
     * @param affectedEntitiesService the service implementation that gets the affected entities.
     * @param moveSucceedRecordExpiredSeconds the move succeeded timeout.
     * @param inProgressActionExpiredSeconds how long to maintain in progress actions for.
     * @param activateSucceedExpiredSeconds how long to keep succeeded activate actions for.
     * @param scaleSucceedRecordExpiredSeconds how long to keep succeeded scale actions for.
     * @param resizeSucceedRecordExpiredSeconds how long to keep succeeded resize actions for.
     */
    public AffectedEntitiesServiceEntityActionDao(
            @Nonnull AffectedEntitiesServiceBlockingStub affectedEntitiesService,
            final int moveSucceedRecordExpiredSeconds,
            final int inProgressActionExpiredSeconds,
            final int activateSucceedExpiredSeconds,
            final int scaleSucceedRecordExpiredSeconds,
            final int resizeSucceedRecordExpiredSeconds) {
        this.affectedEntitiesService = affectedEntitiesService;
        this.moveSucceedRecordExpiredMsec = TimeUnit.SECONDS.toMillis(moveSucceedRecordExpiredSeconds);
        this.inProgressActionExpiredMsec = TimeUnit.SECONDS.toMillis(inProgressActionExpiredSeconds);
        this.activateSucceedExpiredMsec = TimeUnit.SECONDS.toMillis(activateSucceedExpiredSeconds);
        this.scaleSucceedRecordExpiredMsec = TimeUnit.SECONDS.toMillis(scaleSucceedRecordExpiredSeconds);
        this.resizeSucceedRecordExpiredMsec = TimeUnit.SECONDS.toMillis(resizeSucceedRecordExpiredSeconds);
    }

    @Override
    public void insertAction(
            final long actionId,
            @Nonnull final ActionType actionType,
            @Nonnull final Set<Long> entityIds) {
        // We don't need it, because AffectedEntitiesService handles persistence of EntityActions.
    }

    @Override
    public void updateActionState(final long actionId, @Nonnull final ActionState newState) {
        // We don't need it, because AffectedEntitiesService handles persistence of EntityActions.
    }

    @Override
    public void deleteMoveActions(@Nonnull final List<Long> entityOids) {
        // We don't need it, because AffectedEntitiesService handles persistence of EntityActions.
    }

    @Override
    public Set<Long> getNonControllableEntityIds() {
        return new HashSet<>(affectedEntitiesService.getAffectedEntities(
                GetAffectedEntitiesRequest.newBuilder()
                        .putRequestedAffections(
                                ActionEffectType.NON_CONTROLLABLE_VALUE,
                                AffectedEntitiesTimeoutConfig.newBuilder()
                                        .setInProgressActionCoolDownMsec(inProgressActionExpiredMsec)
                                        .setCompletedActionCoolDownMsec(moveSucceedRecordExpiredMsec)
                                        .build())
                        .build())
                .getAffectedEntitiesMap()
                .get(ActionEffectType.NON_CONTROLLABLE_VALUE)
                .getEntityIdList());
    }

    @Override
    public Set<Long> getNonSuspendableEntityIds() {
        return new HashSet<>(affectedEntitiesService.getAffectedEntities(
                GetAffectedEntitiesRequest.newBuilder()
                        .putRequestedAffections(
                                ActionEffectType.NON_SUSPENDABLE_VALUE,
                                AffectedEntitiesTimeoutConfig.newBuilder()
                                        .setInProgressActionCoolDownMsec(inProgressActionExpiredMsec)
                                        .setCompletedActionCoolDownMsec(activateSucceedExpiredMsec)
                                        .build())
                        .build())
                .getAffectedEntitiesMap()
                .get(ActionEffectType.NON_SUSPENDABLE_VALUE)
                .getEntityIdList());
    }

    @Override
    public Set<Long> ineligibleForScaleEntityIds() {
        return new HashSet<>(affectedEntitiesService.getAffectedEntities(
                GetAffectedEntitiesRequest.newBuilder()
                        .putRequestedAffections(
                                ActionEffectType.INELIGIBLE_SCALE_VALUE,
                                AffectedEntitiesTimeoutConfig.newBuilder()
                                        .setInProgressActionCoolDownMsec(inProgressActionExpiredMsec)
                                        .setCompletedActionCoolDownMsec(scaleSucceedRecordExpiredMsec)
                                        .build())
                        .build())
                .getAffectedEntitiesMap()
                .get(ActionEffectType.INELIGIBLE_SCALE_VALUE)
                .getEntityIdList());
    }

    @Override
    public Set<Long> ineligibleForResizeDownEntityIds() {
        return new HashSet<>(affectedEntitiesService.getAffectedEntities(
                GetAffectedEntitiesRequest.newBuilder()
                        .putRequestedAffections(
                                ActionEffectType.INELIGIBLE_RESIZE_DOWN_VALUE,
                                AffectedEntitiesTimeoutConfig.newBuilder()
                                        .setInProgressActionCoolDownMsec(inProgressActionExpiredMsec)
                                        .setCompletedActionCoolDownMsec(
                                                resizeSucceedRecordExpiredMsec)
                                        .build())
                        .build())
                .getAffectedEntitiesMap()
                .get(ActionEffectType.INELIGIBLE_RESIZE_DOWN_VALUE)
                .getEntityIdList());
    }
}
