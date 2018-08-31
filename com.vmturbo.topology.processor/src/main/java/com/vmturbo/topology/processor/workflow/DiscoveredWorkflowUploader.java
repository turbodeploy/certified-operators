package com.vmturbo.topology.processor.workflow;

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

import com.vmturbo.common.protobuf.workflow.DiscoveredWorkflowServiceGrpc;
import com.vmturbo.common.protobuf.workflow.DiscoveredWorkflowServiceGrpc.DiscoveredWorkflowServiceBlockingStub;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.StoreDiscoveredWorkflowsRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowInfo;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;

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
     * For the given target, scan the NonMarketEntityDTOs looking for looking for type == WORKFLOW.
     * We capture those in a map keyed by target OID. Note that any prior Workflows for the same
     * target OID will be discarded.
     * During topology broadcast the 'uploadDiscoveredWorkflows()' method below will be called
     * and after the upload the cache will be reset.
     *
     * @param targetId the target OID from which these NonMarketEntityDTOs were discovered
     * @param nonMarketEntityDTOS the NonMarketEntityDTOs to scan looking for Workflows
     */
    public void setTargetWorkflows(final long targetId,
                                   @Nonnull final List<NonMarketEntityDTO> nonMarketEntityDTOS) {
        log.debug("setTargetWorkflows: {}, size: {}", targetId, nonMarketEntityDTOS.size());
        synchronized (workflowByTarget) {
            List<WorkflowInfo> interpretedWorkflows = discoveredWorkflowInterpreter
                    .interpretWorkflowList(nonMarketEntityDTOS, targetId);
            workflowByTarget.put(targetId, interpretedWorkflows);
        }
    }

    /**
     * If a Target is removed, clear the local cache of Workflows discovered from that target,
     * if any.
     *
     * @param targetId the OID of the target that was deleted
     */
    public void targetRemoved(long targetId) {
        log.debug("target removed: {}", targetId);
        synchronized (workflowByTarget) {
            workflowByTarget.remove(targetId);
        }
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
                    StoreDiscoveredWorkflowsRequest.Builder storeDiscoveredWorkflowsRequest =
                            StoreDiscoveredWorkflowsRequest.newBuilder();
                    storeDiscoveredWorkflowsRequest.setTargetId(targetId);
                    storeDiscoveredWorkflowsRequest.addAllDiscoveredWorkflow(workflows);
                    log.info("upload discovered workflow for {} size: {}",
                            storeDiscoveredWorkflowsRequest.getTargetId(),
                            storeDiscoveredWorkflowsRequest.getDiscoveredWorkflowCount());
                    requests.add(storeDiscoveredWorkflowsRequest.build());
                }
            });
            workflowByTarget.clear();
        }
        requests.forEach(uploadWorkflowsStub::storeDiscoveredWorkflows);
    }
}

