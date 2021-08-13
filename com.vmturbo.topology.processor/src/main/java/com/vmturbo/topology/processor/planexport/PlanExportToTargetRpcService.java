package com.vmturbo.topology.processor.planexport;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus.PlanExportState;
import com.vmturbo.common.protobuf.topology.PlanExport.PlanExportToTargetRequest;
import com.vmturbo.common.protobuf.topology.PlanExport.PlanExportToTargetResponse;
import com.vmturbo.common.protobuf.topology.PlanExportToTargetServiceGrpc.PlanExportToTargetServiceImplBase;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.PlanExport.PlanExportDTO;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.api.PlanExportNotificationListener;
import com.vmturbo.topology.processor.conversions.TopologyToSdkEntityConverter;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.topology.pipeline.CachedTopology;

/**
 * A service that manages exporting plans to targets.
 */
public class PlanExportToTargetRpcService extends PlanExportToTargetServiceImplBase {
    private final IOperationManager operationManager;
    private final PlanExportNotificationListener notificationSender;
    private final ActionsServiceBlockingStub actionsServiceBlockingStub;
    private final ExecutorService exportExecutor;
    private final PlanExportDumper dumper;
    private final TopologyToSdkEntityConverter entityConverter;
    private final RepositoryClient repositoryClient;

    /**
     * Description message for a newly started plan export status.
     */
    public static final String STARTING_EXPORT_MESSAGE = "Starting plan export";

    private static final File PLAN_EXPORT_DUMP_DIRECTORY = new File("/tmp/planexport");

    /**
     * Construct PlanExportToTargetRpcService.
     *
     * @param operationManager manages the plan export operations with a target
     * @param notificationSender receives notifications about export operation execution
     * @param entityConverter converter for entities from platform to SDK DTOs
     * @param repositoryClient for retrieving entities involved in the plan
     * @param actionsServiceBlockingStub used to retrieve action plans from the action orchestrator
     * @param exportExecutor used to run exports on a thread
     * @param dumper An optional PlanExportDumper to record details about the plan export for
     *               debugging purposes.
     */
    public PlanExportToTargetRpcService(@Nonnull final IOperationManager operationManager,
                                        @Nonnull final PlanExportNotificationListener notificationSender,
                                        @Nonnull final TopologyToSdkEntityConverter entityConverter,
                                        @Nonnull final RepositoryClient repositoryClient,
                                        @Nonnull final ActionsServiceBlockingStub actionsServiceBlockingStub,
                                        @Nonnull final ExecutorService exportExecutor,
                                        @Nullable final PlanExportDumper dumper) {
        this.operationManager = operationManager;
        this.notificationSender = notificationSender;
        this.entityConverter = entityConverter;
        this.repositoryClient = repositoryClient;
        this.actionsServiceBlockingStub = actionsServiceBlockingStub;
        this.exportExecutor = exportExecutor;
        this.dumper = dumper;
    }

    /**
     * Initiate a plan export operation to a target. An immediate status will be returned, then
     * the export will run asynchronously, with progress and eventual success or failure indicated
     * by notifications.
     *
     * @param request describes the export to be performed
     * @param responseObserver used to indicate that the export has started, or was immediately
     *                         rejected or failed.
     */
    public void exportPlan(@Nonnull PlanExportToTargetRequest request,
                           @Nonnull StreamObserver<PlanExportToTargetResponse> responseObserver) {
        final EntityRetriever entityRetriever = new EntityRetriever(
            entityConverter,
            repositoryClient,
            new CachedTopology(true),
            request.getPlan().getPlanId());

        exportPlan(request, responseObserver, new PlanExportHelper(entityRetriever));
    }

    /**
     * Initiate a plan export operation to a target. For testing, this variant allows a
     * PlanExportHelper to be passed in.
     *
     * @param request describes the export to be performed
     * @param responseObserver used to indicate that the export has started, or was immediately
     *                         rejected or failed.
     * @param helper a PlanExportHelper to use for plan data conversion for this request
     */
    @VisibleForTesting
    void exportPlan(@Nonnull PlanExportToTargetRequest request,
                    @Nonnull StreamObserver<PlanExportToTargetResponse> responseObserver,
                    @Nonnull PlanExportHelper helper) {
        exportExecutor.execute(new PlanExportRunner(
            request.getPlan(), request.getDestination(), helper, responseObserver));
    }

    /**
     * Simulate execution of a plan export. Temporary to enable UI development and testing
     * before the actual export is implemented.
     */
    public class PlanExportRunner implements Runnable {
        private final Logger logger = LogManager.getLogger();

        private final PlanInstance plan;
        private final PlanDestination destination;
        private final PlanExportHelper helper;
        private StreamObserver<PlanExportToTargetResponse> responseObserver;

        /**
         * Create a runnable that will simulate plan export.
         *
         * @param plan the plan to export
         * @param destination the destination to which the plan should be exported.
         * @param helper a utility for converting plan data for export.
         * @param responseObserver to return the result.
         */
        public PlanExportRunner(@Nonnull final PlanInstance plan,
                                @Nonnull final PlanDestination destination,
                                @Nonnull final PlanExportHelper helper,
                                @Nonnull final StreamObserver<PlanExportToTargetResponse> responseObserver) {
            this.plan = plan;
            this.destination = destination;
            this.helper = helper;
            this.responseObserver = responseObserver;
        }

        @Override
        public void run() {
            logger.debug("Starting PlanExportRunner for plan {} destination {}", plan.getPlanId(),
                destination.getOid());

            // Indicate that we're starting the export. Future updates will be sent asynchronously.
            responseObserver.onNext(PlanExportToTargetResponse.newBuilder()
                .setStatus(startingExport()).build());
            responseObserver.onCompleted();
            responseObserver = null;

            try {
                PlanExportDTO planData = helper.buildPlanExportDTO(plan, actionsForContext(plan.getPlanId()));
                NonMarketEntityDTO destinationEntity = helper.buildPlanDestinationNonMarketEntityDTO(destination);

                logger.debug("Calling OperationManager to initiate upload for plan {} destination {}",
                    plan.getPlanId(), destination.getOid());

                if (dumper != null && logger.isDebugEnabled()) {
                    dumper.dumpPlanExportDetails(PLAN_EXPORT_DUMP_DIRECTORY,
                        destinationEntity, destination.getOid(), planData, plan.getPlanId());
                }

                operationManager.exportPlan(planData, destinationEntity, destination.getOid(),
                    destination.getTargetId());
            } catch (Exception ex) {
                logger.error("Error exporting plan", ex);
                notificationSender.onPlanExportStateChanged(
                    destination.getOid(),
                    PlanExportStatus.newBuilder()
                        .setState(PlanExportState.FAILED)
                        .setDescription(ex.toString())
                        .setProgress(0)
                        .build());
            }
        }

        private PlanExportStatus startingExport() {
            return PlanExportStatus.newBuilder()
                .setState(PlanExportState.IN_PROGRESS)
                .setDescription(STARTING_EXPORT_MESSAGE)
                .setProgress(0).build();
        }

        private List<Action> actionsForContext(long topologyContextId) {
            FilteredActionRequest actionsRequest = FilteredActionRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .setPaginationParams(PaginationParameters.newBuilder().setEnforceLimit(false))
                .build();

            logger.debug("Calling Action Orchestrator to fetch actions for plan {} destination {}",
                plan.getPlanId(), destination.getOid());

            List<Action> actions = new ArrayList<>();
            Iterator<FilteredActionResponse> responseIterator
                = actionsServiceBlockingStub.getAllActions(actionsRequest);

            while (responseIterator.hasNext()) {
                FilteredActionResponse response = responseIterator.next();
                for (ActionOrchestratorAction action : response.getActionChunk().getActionsList()) {
                    actions.add(action.getActionSpec().getRecommendation());
                }
            }

            logger.debug("Fetched {} actions for plan {} destination {}",
                actions.size(), plan.getPlanId(), destination.getOid());

            return actions;
        }
    }
}

