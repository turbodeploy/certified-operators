package com.vmturbo.topology.processor.workflow;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.workflow.DiscoveredWorkflowServiceGrpc;
import com.vmturbo.common.protobuf.workflow.DiscoveredWorkflowServiceGrpc.DiscoveredWorkflowServiceBlockingStub;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.StoreDiscoveredWorkflowsRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.platform.common.dto.ActionExecution;

/**
 * Record any Workflows extracted from discovery results in an in-memory map, by TargetId, until
 * the topology is broadcast. At that time, as one of the topology processing stages, the
 * "uploadDiscoveredWorkflows()" method will be called to send all the discovered workflows
 * collected to the Action Orchestrator, where they will be persisted.
 **/
@ThreadSafe
public class DiscoveredWorkflowUploader {

    // map from a target ID to the most recent workflow discovered from that target.
    private final Map<Long, List<WorkflowInfo>> workflowByTarget = Maps.newHashMap();

    // RPC stub for uploading workflow
    private final DiscoveredWorkflowServiceBlockingStub uploadWorkflowsStub;

    private Logger log = LogManager.getLogger();
    private DiscoveredWorkflowInterpreter discoveredWorkflowInterpreter;

    /**
     * Create a service to extract Workflows from a DiscoveryResult and store them until
     * a Topology Broadcast, at which time they will be uploaded to Action Orchestrator,
     * which is responsible for Workflow persistence.
     *
     * @param actionOrchestratorChannel the gRPC channel for the Action Orchestrator
     * @param discoveredWorkflowInterpreter a scanner to detect Workflow-specific NonMarketEntities
     *                                      in the Discovery Response and convert them to WorkflowInfo
     *                                      objects.
     */
    public DiscoveredWorkflowUploader(@Nonnull Channel actionOrchestratorChannel,
                                      @Nonnull DiscoveredWorkflowInterpreter discoveredWorkflowInterpreter) {
        this.uploadWorkflowsStub = DiscoveredWorkflowServiceGrpc.newBlockingStub(
                Objects.requireNonNull(actionOrchestratorChannel));
        this.discoveredWorkflowInterpreter = Objects.requireNonNull(discoveredWorkflowInterpreter);
    }

    /**
     * For the given target, create a map of workflows keyed by target OID.
     *
     * <p>Note that any prior Workflows for the same target OID will be discarded.</p>
     *
     * <p></p>During topology broadcast the 'uploadDiscoveredWorkflows()' method below will be called
     * and after the upload the cache will be reset.</p>
     *
     * @param targetId the target OID from which these NonMarketEntityDTOs were discovered
     * @param workflows the NonMarketEntityDTOs to scan looking for Workflows
     */
    public void setTargetWorkflows(final long targetId,
                                   @Nonnull final List<ActionExecution.Workflow> workflows) {
        log.debug("setTargetWorkflows: {}, size: {}", targetId, workflows.size());
        synchronized (workflowByTarget) {
            List<WorkflowInfo> interpretedWorkflows = discoveredWorkflowInterpreter
                    .interpretWorkflowList(workflows, targetId);
            workflowByTarget.put(targetId, interpretedWorkflows);
        }
    }

    /**
     * When a Target is removed, notify Action Orchestrator and clear the local cache of Workflows
     * discovered from that target, if any.
     *
     * @param targetId the OID of the target that was deleted
     */
    public void targetRemoved(long targetId) {
        try {
            // Notify the Action Orchestrator that all workflows related to this target should be deleted
            final StoreDiscoveredWorkflowsRequest emptyWorkflowRequest =
                    createWorkflowRequest(targetId, Collections.emptyList());
            uploadWorkflowsStub.storeDiscoveredWorkflows(emptyWorkflowRequest);
        } catch (RuntimeException e) {
            if (e instanceof StatusRuntimeException) {
                Status status = ((StatusRuntimeException)e).getStatus();
                log.warn("Unable to delete workflow: {} caused by {}.",
                        status.getDescription(), status.getCause());
            } else {
                log.error("Error deleting workflow.", e);
            }
        }
        // clear the local cache of workflows discovered from the target, if any.
        synchronized (workflowByTarget) {
            workflowByTarget.remove(targetId);
        }
        log.debug("target removed: {}", targetId);
    }

    /**
     * Upload the currently discovered Workflows to the Action Orchestrator.
     * We build one upload request for each target from the current discovery cache and save the upload
     * requests in a list; and then we reset the cache for next time.
     *
     * Finally, we send the update request for one target at a time.
     *
     * By a list of upload requests w minimizes the computation done in the synchronized() block.
     *
     * We could consider sending the upload requests in a single stream-parameter gRPC call down the road.
     *
     */
    public void uploadDiscoveredWorkflows() {
        List<StoreDiscoveredWorkflowsRequest> requests = Lists.newArrayList();
        synchronized (workflowByTarget) {
            workflowByTarget.forEach((targetId, workflows) -> {
                if (!workflows.isEmpty()) {
                    requests.add(createWorkflowRequest(targetId, workflows));
                }
            });
            workflowByTarget.clear();
        }
        requests.forEach(uploadWorkflowsStub::storeDiscoveredWorkflows);
    }

    private StoreDiscoveredWorkflowsRequest createWorkflowRequest(final Long targetId,
            final List<WorkflowInfo> workflows) {
        StoreDiscoveredWorkflowsRequest.Builder storeDiscoveredWorkflowsRequest =
                StoreDiscoveredWorkflowsRequest.newBuilder();
        storeDiscoveredWorkflowsRequest.setTargetId(targetId);
        storeDiscoveredWorkflowsRequest.addAllDiscoveredWorkflow(workflows);
        log.info("upload discovered workflow for {} size: {}",
                storeDiscoveredWorkflowsRequest.getTargetId(),
                storeDiscoveredWorkflowsRequest.getDiscoveredWorkflowCount());
        return storeDiscoveredWorkflowsRequest.build();
    }
}

