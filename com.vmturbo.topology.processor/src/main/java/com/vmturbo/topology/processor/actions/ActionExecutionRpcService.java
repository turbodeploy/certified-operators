package com.vmturbo.topology.processor.actions;

import static com.vmturbo.common.protobuf.ActionDTOUtil.getProviderEntityIdsFromMoveAction;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jersey.repackaged.com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionResponse;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc.ActionExecutionServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.CommodityAttribute;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.controllable.EntityActionDao;
import com.vmturbo.topology.processor.entity.Entity.PerTargetInfo;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;

public class ActionExecutionRpcService extends ActionExecutionServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Tokens to use generating error logging during entity lookup. These correspond to the
     * three entity designations in any action.
     */
    private static final String TARGET_LOOKUP_TOKEN = "target";
    private static final String SOURCE_LOOKUP_TOKEN = "source";
    private static final String DESTINATION_LOOKUP_TOKEN = "destination";

    private final EntityStore entityStore;

    private final IOperationManager operationManager;

    private final EntityActionDao entityActionDao;

    public ActionExecutionRpcService(@Nonnull final EntityStore entityStore,
                    @Nonnull final IOperationManager operationManager,
                    @Nonnull final EntityActionDao entityActionDao) {
        this.entityStore = Objects.requireNonNull(entityStore);
        this.operationManager = Objects.requireNonNull(operationManager);
        this.entityActionDao = Objects.requireNonNull(entityActionDao);
    }

    @Override
    public void executeAction(ExecuteActionRequest request,
                    StreamObserver<ExecuteActionResponse> responseObserver) {
        try {

            final ActionInfo actionInfo = request.getActionInfo();
            final List<ActionItemDTO> sdkActions = Lists.newArrayList();
            switch (actionInfo.getActionTypeCase()) {
                case MOVE:
                    sdkActions.addAll(move(request.getTargetId(),
                        request.getActionId(), actionInfo.getMove()));
                    break;
                case RESIZE:
                    sdkActions.add(resize(request.getTargetId(),
                        request.getActionId(), actionInfo.getResize()));
                    break;
                case ACTIVATE:
                    sdkActions.add(activate(request.getTargetId(),
                        request.getActionId(), actionInfo.getActivate()));
                    break;
                case DEACTIVATE:
                    sdkActions.add(deactivate(request.getTargetId(),
                        request.getActionId(), actionInfo.getDeactivate()));
                    break;
                case PROVISION:
                    sdkActions.add(provision(request.getTargetId(),
                            request.getActionId(), actionInfo.getProvision()));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported action type: " +
                                    actionInfo.getActionTypeCase());
            }

            logger.debug("Start action {}", sdkActions);
            Optional<WorkflowDTO.WorkflowInfo> workflowOptional = request.hasWorkflowInfo() ?
                    Optional.of(request.getWorkflowInfo()) : Optional.empty();

            operationManager.requestActions(request.getActionId(),
                    request.getTargetId(),
                    Objects.requireNonNull(sdkActions),
                    workflowOptional);

            responseObserver.onNext(ExecuteActionResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (ProbeException e) {
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription(e.getMessage()).asException());
        } catch (InterruptedException e) {
            responseObserver.onError(Status.ABORTED
                    .withDescription(e.getMessage()).asException());
        } catch (ActionExecutionException | TargetNotFoundException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage()).asException());
        } catch (CommunicationException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage()).asException());
        }
    }

    @Nonnull
    public ActionItemDTO activate(final long targetId,
                    final long actionId,
                    @Nonnull final ActionDTO.Activate activateAction)
                                    throws ActionExecutionException {
        try {
            // insert activate actions to action record table
            entityActionDao.insertAction(actionId, ActionDTO.ActionType.ACTIVATE,
                    new HashSet<>(Collections.singletonList(targetId)));
        } catch (DataAccessException e) {
            logger.error("Failed to create queued activate action records for action: {}",
                    actionId);
        }
        final PerTargetInfo targetInfo = getPerTargetInfo(targetId,
                activateAction.getTarget().getId(), TARGET_LOOKUP_TOKEN);

        final ActionItemDTO.Builder actionBuilder = ActionItemDTO.newBuilder();
        actionBuilder.setActionType(ActionType.START);
        actionBuilder.setUuid(Long.toString(actionId));
        actionBuilder.setTargetSE(targetInfo.getEntityInfo());

        getHost(targetId, targetInfo).ifPresent(actionBuilder::setHostedBySE);

        return actionBuilder.build();
    }

    @Nonnull
    public ActionItemDTO deactivate(final long targetId,
                    final long actionId,
                    @Nonnull final ActionDTO.Deactivate deactivateAction)
                                    throws ActionExecutionException {
        final PerTargetInfo targetInfo = getPerTargetInfo(targetId,
                deactivateAction.getTarget().getId(), TARGET_LOOKUP_TOKEN);

        final ActionItemDTO.Builder actionBuilder = ActionItemDTO.newBuilder();
        actionBuilder.setActionType(ActionType.SUSPEND);
        actionBuilder.setUuid(Long.toString(actionId));
        actionBuilder.setTargetSE(targetInfo.getEntityInfo());

        getHost(targetId, targetInfo).ifPresent(actionBuilder::setHostedBySE);

        return actionBuilder.build();
    }

    @Nonnull
    public ActionItemDTO provision(final long targetId,
                    final long actionId,
                    @Nonnull final ActionDTO.Provision provision)
                                    throws ActionExecutionException {
        final PerTargetInfo targetInfo = getPerTargetInfo(targetId,
                provision.getEntityToClone().getId(), TARGET_LOOKUP_TOKEN);

        final ActionItemDTO.Builder actionBuilder = ActionItemDTO.newBuilder();
        actionBuilder.setActionType(ActionType.PROVISION);
        actionBuilder.setUuid(Long.toString(actionId));
        actionBuilder.setTargetSE(targetInfo.getEntityInfo());

        getHost(targetId, targetInfo).ifPresent(actionBuilder::setHostedBySE);

        return actionBuilder.build();
    }


    @Nonnull
    public ActionItemDTO resize(final long targetId,
                    final long actionId,
                    @Nonnull final ActionDTO.Resize resizeAction)
                                    throws ActionExecutionException {
        final PerTargetInfo targetInfo = getPerTargetInfo(targetId, resizeAction.getTarget().getId(),
                TARGET_LOOKUP_TOKEN);

        final ActionItemDTO.Builder actionBuilder = ActionItemDTO.newBuilder();

        // TODO (roman, May 16  2017): At the time of this writing, most probes expect RIGHT_SIZE,
        // and a few expect RESIZE. Need to remove the inconsistencies, especially if we want to
        // make this usable with third-party probes in the future.
        actionBuilder.setActionType(ActionType.RIGHT_SIZE);

        actionBuilder.setUuid(Long.toString(actionId));
        actionBuilder.setTargetSE(targetInfo.getEntityInfo());

        getHost(targetId, targetInfo).ifPresent(actionBuilder::setHostedBySE);

        // Handle resizes on commodity attributes other than just capacity. Note that we
        // are converting TopologyDTO.CommodityAttribute enum to a classic ActionExecution
        // .ActionItemDTO.CommodityAttribute enum here -- we are matching on the ordinal number,
        // which is set up (but not enforced) to match in the two enums.
        actionBuilder.setCommodityAttribute(
                CommodityAttribute.forNumber(resizeAction.getCommodityAttribute().getNumber()));

        actionBuilder.setCurrentComm(commodityBuilderFromType(resizeAction.getCommodityType())
            .setCapacity(resizeAction.getOldCapacity()));

        // TODO (roman, May 15 2017): Right now we forward the capacity from the resize
        // action directly to the probe. We may need to round things off to certain increments
        // depending on which commodity we're dealing with. This will be partially mitigated
        // by sending correct increments to the market. (OM-16571)
        actionBuilder.setNewComm(commodityBuilderFromType(resizeAction.getCommodityType())
            .setCapacity(resizeAction.getNewCapacity()));

        return actionBuilder.build();
    }

    @Nonnull
    public List<ActionItemDTO> move(final long targetId,
                    final long actionId,
                    @Nonnull final ActionDTO.Move moveAction) throws ActionExecutionException {
        final Set<Long> entityIds = getProviderEntityIdsFromMoveAction(moveAction);
        try {
            // insert records to controllable table which have "queued" status.
            entityActionDao.insertAction(actionId, ActionDTO.ActionType.MOVE, entityIds);
        } catch (DataAccessException e) {
            logger.error("Failed to create queued controllable records for action: {}", actionId);
        }
        List<ActionItemDTO> actions = Lists.newArrayList();
        final PerTargetInfo targetInfo = getPerTargetInfo(targetId, moveAction.getTarget().getId(),
                TARGET_LOOKUP_TOKEN);
        for (ChangeProvider change : moveAction.getChangesList()) {
            actions.add(actionItemDto(targetId, change, actionId, targetInfo));
        }
        return actions;
    }

    private ActionItemDTO actionItemDto(long targetId, ChangeProvider change, long actionId,
                                        PerTargetInfo targetInfo) throws ActionExecutionException {
        final PerTargetInfo sourceInfo = getPerTargetInfo(targetId, change.getSource().getId(),
                SOURCE_LOOKUP_TOKEN);
        final PerTargetInfo destInfo = getPerTargetInfo(targetId, change.getDestination().getId(),
                DESTINATION_LOOKUP_TOKEN);

        // Set the action type depending on the type of the entity being moved.
        final EntityType srcEntityType = sourceInfo.getEntityInfo().getEntityType();
        if (srcEntityType != destInfo.getEntityInfo().getEntityType()) {
            throw new ActionExecutionException("Mismatched source and destination entity types! " +
                            " Source: " + srcEntityType +
                            " Destination: " + sourceInfo.getEntityInfo().getEntityType());
        }

        final ActionItemDTO.Builder actionBuilder = ActionItemDTO.newBuilder()
                        .setActionType(srcEntityType == EntityType.STORAGE
                        ? ActionType.CHANGE
                            : ActionType.MOVE)
                        .setUuid(Long.toString(actionId))
                        .setTargetSE(targetInfo.getEntityInfo())
                        .setCurrentSE(sourceInfo.getEntityInfo())
                        .setNewSE(destInfo.getEntityInfo());

        getHost(targetId, targetInfo).ifPresent(actionBuilder::setHostedBySE);
        return actionBuilder.build();
    }

    private Optional<EntityDTO> getHost(final long targetId, final PerTargetInfo entityInfo)
                    throws ActionExecutionException {
        // Right now hosted by only gets set for VM -> PM and Container -> Pod relationships.
        // TODO (roman, May 16 2017): Generalize to all entities where this is necessary, tracked in OM-40025
        if (entityInfo.getEntityInfo().getEntityType().equals(EntityType.VIRTUAL_MACHINE) ||
                entityInfo.getEntityInfo().getEntityType().equals(EntityType.CONTAINER)) {
            final PerTargetInfo hostOfTarget = getPerTargetInfo(targetId,
                entityInfo.getHost(), "host of target");
            return Optional.of(hostOfTarget.getEntityInfo());
        }
        return Optional.empty();
    }

    private CommodityDTO.Builder commodityBuilderFromType(@Nonnull final CommodityType commodityType) {
        return CommodityDTO.newBuilder()
                        .setCommodityType(CommodityDTO.CommodityType.forNumber(commodityType.getType()))
                        .setKey(commodityType.getKey());
    }

    private PerTargetInfo getPerTargetInfo(final long targetId,
                    final long entityId,
                    final String entityType)
                                    throws ActionExecutionException {
        return entityStore.getEntity(entityId)
                        .orElseThrow(() -> ActionExecutionException.noEntity(entityType, entityId))
                        .getEntityInfo(targetId)
                        .orElseThrow(() -> ActionExecutionException.noEntityTargetInfo(
                            entityType, entityId, targetId));
    }
}
