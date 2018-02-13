package com.vmturbo.topology.processor.actions;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionResponse;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc.ActionExecutionServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.CommodityAttribute;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.entity.Entity.PerTargetInfo;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;

public class ActionExecutionRpcService extends ActionExecutionServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private final EntityStore entityStore;

    private final IOperationManager operationManager;

    public ActionExecutionRpcService(@Nonnull final EntityStore entityStore,
                                     @Nonnull final IOperationManager operationManager) {
        this.entityStore = Objects.requireNonNull(entityStore);
        this.operationManager = Objects.requireNonNull(operationManager);
    }

    @Override
    public void executeAction(ExecuteActionRequest request,
                              StreamObserver<ExecuteActionResponse> responseObserver) {
        try {

            final ActionInfo actionInfo = request.getActionInfo();
            final ActionItemDTO sdkAction;
            switch (actionInfo.getActionTypeCase()) {
                case MOVE:
                    sdkAction = move(request.getTargetId(),
                            request.getActionId(), actionInfo.getMove());
                    break;
                case RESIZE:
                    sdkAction = resize(request.getTargetId(),
                            request.getActionId(), actionInfo.getResize());
                    break;
                case ACTIVATE:
                    sdkAction = activate(request.getTargetId(),
                            request.getActionId(), actionInfo.getActivate());
                    break;
                case DEACTIVATE:
                    sdkAction = deactivate(request.getTargetId(),
                            request.getActionId(), actionInfo.getDeactivate());
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported action type: " +
                            actionInfo.getActionTypeCase());
            }

            operationManager.startAction(request.getActionId(),
                    request.getTargetId(),
                    Objects.requireNonNull(sdkAction));

            responseObserver.onNext(ExecuteActionResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (ProbeException e) {
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription(e.getMessage()).asException());
        } catch (InterruptedException e) {
            responseObserver.onError(Status.ABORTED
                    .withDescription(e.getMessage()).asException());
        } catch (ActionExecutionException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage()).asException());
        } catch (TargetNotFoundException e) {
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
        final PerTargetInfo targetInfo =
                getPerTargetInfo(targetId, activateAction.getTargetId(), "target");

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
        final PerTargetInfo targetInfo =
                getPerTargetInfo(targetId, deactivateAction.getTargetId(), "target");

        final ActionItemDTO.Builder actionBuilder = ActionItemDTO.newBuilder();
        actionBuilder.setActionType(ActionType.SUSPEND);
        actionBuilder.setUuid(Long.toString(actionId));
        actionBuilder.setTargetSE(targetInfo.getEntityInfo());

        getHost(targetId, targetInfo).ifPresent(actionBuilder::setHostedBySE);

        return actionBuilder.build();
    }


    @Nonnull
    public ActionItemDTO resize(final long targetId,
                         final long actionId,
                         @Nonnull final ActionDTO.Resize resizeAction)
            throws InterruptedException, ProbeException, TargetNotFoundException,
                CommunicationException, ActionExecutionException {
        final PerTargetInfo targetInfo =
                getPerTargetInfo(targetId, resizeAction.getTargetId(), "target");

        final ActionItemDTO.Builder actionBuilder = ActionItemDTO.newBuilder();

        // TODO (roman, May 16  2017): At the time of this writing, most probes expect RIGHT_SIZE,
        // and a few expect RESIZE. Need to remove the inconsistencies, especially if we want to
        // make this usable with third-party probes in the future.
        actionBuilder.setActionType(ActionType.RIGHT_SIZE);

        actionBuilder.setUuid(Long.toString(actionId));
        actionBuilder.setTargetSE(targetInfo.getEntityInfo());

        getHost(targetId, targetInfo).ifPresent(actionBuilder::setHostedBySE);

        actionBuilder.setCommodityAttribute(CommodityAttribute.Capacity);

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
    public ActionItemDTO move(final long targetId,
                              final long actionId,
                              @Nonnull final ActionDTO.Move moveAction)
            throws InterruptedException, ProbeException,
            TargetNotFoundException, CommunicationException, ActionExecutionException {

        final PerTargetInfo targetInfo = getPerTargetInfo(targetId, moveAction.getTargetId(), "target");
        // TODO(COMPOUND): handle action with multiple changes
        final PerTargetInfo sourceInfo = getPerTargetInfo(targetId, moveAction.getChanges(0).getSourceId(), "source");
        final PerTargetInfo destInfo = getPerTargetInfo(targetId, moveAction.getChanges(0).getDestinationId(), "destination");

        final ActionItemDTO.Builder actionBuilder = ActionItemDTO.newBuilder();

        // Set the action type depending on the type of the entity being moved.
        {
            final EntityType srcEntityType = sourceInfo.getEntityInfo().getEntityType();
            if (srcEntityType != destInfo.getEntityInfo().getEntityType()) {
                throw new ActionExecutionException("Mismatched source and destination entity types! " +
                        " Source: " + srcEntityType +
                        " Destination: " + sourceInfo.getEntityInfo().getEntityType());
            }

            // The specific case of VM move on a Storage is considered a "CHANGE" action but
            // all others are considered "MOVE"
            switch (srcEntityType) {
                case STORAGE:
                    actionBuilder.setActionType(ActionType.CHANGE);
                    break;
                default:
                    actionBuilder.setActionType(ActionType.MOVE);
                    break;
            }
        }

        actionBuilder.setUuid(Long.toString(actionId));
        actionBuilder.setTargetSE(targetInfo.getEntityInfo());

        getHost(targetId, targetInfo).ifPresent(actionBuilder::setHostedBySE);

        actionBuilder.setCurrentSE(sourceInfo.getEntityInfo());

        actionBuilder.setNewSE(destInfo.getEntityInfo());

        return actionBuilder.build();
    }


    private Optional<EntityDTO> getHost(final long targetId, final PerTargetInfo entityInfo)
            throws ActionExecutionException {
        // Right now hosted by only gets set for VM -> PM relationships.
        // This is most notably required by the HyperV probe.
        // TODO (roman, May 16 2017): Generalize to all entities where this is necessary.
        if (entityInfo.getEntityInfo().getEntityType().equals(EntityType.VIRTUAL_MACHINE)) {
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
                .getTargetInfo(targetId)
                .orElseThrow(() -> ActionExecutionException.noEntityTargetInfo(
                        entityType, entityId, targetId));
    }
}
