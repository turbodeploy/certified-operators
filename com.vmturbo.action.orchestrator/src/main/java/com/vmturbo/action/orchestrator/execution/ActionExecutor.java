package com.vmturbo.action.orchestrator.execution;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;

import com.vmturbo.action.orchestrator.action.ExecutableStep;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc;
import com.vmturbo.common.protobuf.topology.ActionExecutionServiceGrpc.ActionExecutionServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass;
import com.vmturbo.common.protobuf.topology.EntityServiceGrpc;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * Executes actions by converting {@link ActionDTO.Action} objects into {@link ExecuteActionRequest}
 * and sending them to the {@link TopologyProcessor}.
 */
public class ActionExecutor {
    private static final Logger logger = LogManager.getLogger();

    private final ActionExecutionServiceBlockingStub actionExecutionService;

    private final EntityServiceGrpc.EntityServiceBlockingStub entityServiceBlockingStub;

    public ActionExecutor(@Nonnull final Channel topologyProcessorChannel) {
        this.actionExecutionService =
                ActionExecutionServiceGrpc.newBlockingStub(topologyProcessorChannel);
        this.entityServiceBlockingStub =
                EntityServiceGrpc.newBlockingStub(topologyProcessorChannel);
    }

    /**
     * Get the ID of the target for the {@link ExecutableStep} for a {@link ActionDTO.Action} recommendation.
     *
     * @param action The {@link ActionDTO.Action} in question.
     * @return The ID of the desired target.
     * @throws TargetResolutionException If there are any errors retrieving the target ID.
     */
    public long getTargetId(@Nonnull final ActionDTO.Action action) throws TargetResolutionException {
        try {
            final Set<Long> involvedEntities = ActionDTOUtil.getInvolvedEntities(action);
            // This shouldn't happen.
            if (involvedEntities.isEmpty()) {
                throw new IllegalStateException("Action: " + action.getId() + " has no involved entities.");
            }
            final Map<Long, EntityInfoOuterClass.EntityInfo> involvedEntityInfos
                    = getEntityInfo(involvedEntities);

            if (!involvedEntityInfos.keySet().containsAll(involvedEntities)) {
                throw new TargetResolutionException("Action: " + action.getId() +
                        " Some entities not found in Topology Processor.");
            }

            // Find the set of targets that discovered all the
            // involved entities.
            Set<Long> overlappingTargetSet = null;
            for (final EntityInfoOuterClass.EntityInfo info : involvedEntityInfos.values()) {
                final Set<Long> curInfoTargets = info.getTargetIdToProbeIdMap().keySet();
                if (overlappingTargetSet == null) {
                    overlappingTargetSet = new HashSet<>(curInfoTargets);
                } else {
                    overlappingTargetSet.retainAll(curInfoTargets);
                }
            }

            if (overlappingTargetSet == null || overlappingTargetSet.isEmpty()) {
                throw new TargetResolutionException("Action: " + action.getId() + " has no overlapping targets between the entities involved.");
            }

            return overlappingTargetSet.iterator().next();
        } catch (UnsupportedActionException e) {
            throw new TargetResolutionException("Action: " + action.getId() + " has unsupported type: " + e.getActionType(), e);
        }
    }

    public void execute(final long targetId, @Nonnull final ActionDTO.Action action)
            throws ExecutionStartException {
        final ExecuteActionRequest executionRequest = ExecuteActionRequest.newBuilder()
                .setActionId(action.getId())
                .setTargetId(targetId)
                .setActionInfo(action.getInfo())
                .build();

        try {
            actionExecutionService.executeAction(executionRequest);
            logger.info("Action: {} started.", action.getId());
        } catch (StatusRuntimeException e) {
            throw new ExecutionStartException("Action: " + action.getId() + " failed to start. Failure status: " + e.getStatus(), e);
        }
    }

    public Map<Long, EntityInfoOuterClass.EntityInfo> getEntityInfo(Set<Long> entityIds) {
        Iterator<EntityInfoOuterClass.EntityInfo> response =
                entityServiceBlockingStub.getEntitiesInfo(EntityInfoOuterClass.GetEntitiesInfoRequest
                        .newBuilder().addAllEntityIds(entityIds).build()
        );

        final Iterable<EntityInfoOuterClass.EntityInfo> iterableResponse = () -> response;
        return StreamSupport.stream(iterableResponse.spliterator(), false)
                .collect(Collectors.toMap(EntityInfoOuterClass.EntityInfo::getEntityId, Function.identity()));
    }
}
