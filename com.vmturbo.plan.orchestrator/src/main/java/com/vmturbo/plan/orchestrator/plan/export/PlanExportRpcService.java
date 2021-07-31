package com.vmturbo.plan.orchestrator.plan.export;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.GetPlanDestinationResponse;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.GetPlanDestinationsRequest;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination.Builder;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestinationID;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportRequest;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportResponse;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus.PlanExportState;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.StoreDiscoveredPlanDestinationsResponse;
import com.vmturbo.common.protobuf.plan.PlanExportServiceGrpc.PlanExportServiceImplBase;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration;
import com.vmturbo.common.protobuf.topology.PlanExport.PlanExportToTargetRequest;
import com.vmturbo.common.protobuf.topology.PlanExport.PlanExportToTargetResponse;
import com.vmturbo.common.protobuf.topology.PlanExportToTargetServiceGrpc.PlanExportToTargetServiceBlockingStub;
import com.vmturbo.plan.orchestrator.plan.IntegrityException;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.export.PlanExportListener.PlanExportListenerException;
import com.vmturbo.topology.processor.api.PlanExportNotificationListener;

/**
 * Implements functionality for plan export service.
 */
public class PlanExportRpcService extends PlanExportServiceImplBase
    implements PlanExportNotificationListener {

    private static final String DISAPPEARED_DURING_UPDATE = "Destination disappeared during update";
    private static final String INTEGRITY_ERROR = "Integrity error";

    private final Logger logger = LogManager.getLogger();

    /**
     * Dao access.
     */
    private final PlanDestinationDao planDestinationDao;
    private final PlanDao planDao;

    private final PlanExportToTargetServiceBlockingStub tpExportService;
    private final PlanExportNotificationSender planExportNotificationSender;

    /**
     * Construct PlanExportRpcService.
     *
     * @param planDestinationDao DAO for accessing PlanDestination table
     * @param planDao DAO for accessing Plan data
     * @param tpExportService blocking stub for making calls to the Topology Processor
     *                        export-to-target service
     * @param sender a PlanExportNotifictionSender.
     */
    public PlanExportRpcService(@Nonnull final PlanDestinationDao planDestinationDao,
                                @Nonnull final PlanDao planDao,
                                @Nonnull final PlanExportToTargetServiceBlockingStub tpExportService,
                                @Nonnull final PlanExportNotificationSender sender) {
        this.planDestinationDao = Objects.requireNonNull(planDestinationDao);
        this.tpExportService = tpExportService;
        this.planDao = planDao;
        this.planExportNotificationSender = sender;
    }

    /**
     * Guards all update operations.
     */
    private final Lock updateLock = new ReentrantLock();

    @Override
    public void getPlanDestination(final PlanDestinationID planDestinationID, final StreamObserver<GetPlanDestinationResponse> responseObserver) {
        Optional<PlanDestination> planDestination = planDestinationDao.getPlanDestination(planDestinationID.getId());
        if (planDestination.isPresent()) {
            responseObserver.onNext(
                GetPlanDestinationResponse.newBuilder()
                    .setDestination(planDestination.get())
                    .build());
        } else {
            responseObserver.onNext(GetPlanDestinationResponse.newBuilder().build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void getPlanDestinations(final GetPlanDestinationsRequest request, final StreamObserver<PlanDestination> responseObserver) {
        List<PlanDestination> planDestinationList = planDestinationDao.getAllPlanDestinations();
        if (!planDestinationList.isEmpty()) {
            if (request.hasAccountId()) {
                final long requestedAccountId = request.getAccountId();
                planDestinationList.stream()
                    .filter(planDestination ->
                        planDestination.hasCriteria() && planDestination.getCriteria().hasAccountId()
                        && planDestination.getCriteria().getAccountId() == requestedAccountId)
                    .forEach(planDestination -> responseObserver.onNext(planDestination));
            } else {
                planDestinationList.stream()
                    .forEach(planDestination -> responseObserver.onNext(planDestination));
            }
        }
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<PlanDestination> storeDiscoveredPlanDestinations(final StreamObserver<StoreDiscoveredPlanDestinationsResponse> responseObserver) {
        return new DiscoveredPlanDestinationStreamObserver(responseObserver);
    }

    /**
     * A stream observer which stores all plan destinations uploaded from topology processor
     * and save to db.
     */
    private class DiscoveredPlanDestinationStreamObserver implements StreamObserver<PlanDestination> {

        private final StreamObserver<StoreDiscoveredPlanDestinationsResponse> responseObserver;
        private List<PlanDestination> planDestinations = new ArrayList<>();

        /**
         * Constructor for {@link DiscoveredPlanDestinationStreamObserver}.
         *
         * @param responseObserver the observer which notifies client of any result
         */
        DiscoveredPlanDestinationStreamObserver(StreamObserver<StoreDiscoveredPlanDestinationsResponse> responseObserver) {
            this.responseObserver = responseObserver;
        }

        @Override
        public void onNext(final PlanDestination record) {
            planDestinations.add(record);
        }

        @Override
        public void onError(final Throwable t) {
            logger.error("Error uploading discovered non-entities", t);
        }

        @Override
        public void onCompleted() {
            updateLock.lock();
            try {
                final List<PlanDestination> originalPlanDestinations = planDestinationDao.getAllPlanDestinations();
                Map<String, PlanDestination> originalPlanDestinationsMap = originalPlanDestinations.stream()
                    .collect(Collectors.toMap(PlanDestination::getExternalId, Function.identity()));
                Set<String> newExternalIds = new HashSet<>();

                planDestinations.stream().forEach(record -> {
                    newExternalIds.add(record.getExternalId());

                    if (originalPlanDestinationsMap.keySet().contains(record.getExternalId())) {
                        PlanDestination oldPlanDestination = originalPlanDestinationsMap.get(record.getExternalId());
                        try {
                            planDestinationDao.updatePlanDestination(oldPlanDestination.getOid(), record);
                        } catch (NoSuchObjectException e) {
                            responseObserver.onError(Status.NOT_FOUND
                                .withDescription("Unable to find pre-existing record: " + e.getMessage()).asException());
                        } catch (IntegrityException e) {
                            responseObserver.onError(Status.UNKNOWN
                                .withDescription("Unable to persist: " + e.getMessage()).asException());
                        }
                    } else {
                        // create new record
                        try {
                            PlanDestination result = planDestinationDao.createPlanDestination(record);
                        } catch (IntegrityException e) {
                            responseObserver.onError(Status.UNKNOWN
                                .withDescription("Unable to persist: " + e.getMessage()).asException());
                        }
                    }
                });

                // Remove all the not exist non-in-progress records from DB
                if (!originalPlanDestinations.isEmpty()) {
                    originalPlanDestinationsMap.entrySet().stream().forEach(entry -> {
                        if (!newExternalIds.contains(entry.getKey())) {
                            PlanDestination planDestination = entry.getValue();
                            if (!PlanExportState.IN_PROGRESS.equals(planDestination.getStatus().getState())) {
                                int result = planDestinationDao.deletePlanDestination(entry.getValue().getOid());
                                if (result == 1) {
                                    logger.error("Unable to remove plan destination {} from DB during cleanup.", planDestination.getExternalId());
                                }
                            }
                        }
                    });
                }
            } finally {
                updateLock.unlock();
            }

            responseObserver.onNext(StoreDiscoveredPlanDestinationsResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void startPlanExport(final PlanExportRequest request,
                                final StreamObserver<PlanExportResponse> responseObserver) {
        PlanExportResponse response = PlanExportResponse.getDefaultInstance();

        updateLock.lock();
        try {
            Optional<PlanDestination> currentPlanDestination =
                planDestinationDao.getPlanDestination(request.getDestinationId());

            if (currentPlanDestination.isPresent()) {
                response = PlanExportResponse.newBuilder()
                    .setDestination(attemptPlanExport(request, currentPlanDestination.get())).build();
            }
        } finally {
            updateLock.unlock();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Nonnull
    private PlanDestination attemptPlanExport(@Nonnull final PlanExportRequest request,
                                              @Nonnull PlanDestination destination) {
        final long destinationOid = destination.getOid();
        Long marketId = request.getMarketId();

        try {
            if (destination.getStatus().getState() != PlanExportState.IN_PROGRESS) {
                Optional<PlanInstance> plan = planDao.getPlanInstance(marketId);
                if (plan.isPresent()) {
                    String errorMessage = validatePlanExport(request, plan.get(), destination);
                    if (errorMessage == null) {
                        try {
                            PlanExportToTargetResponse response = tpExportService.exportPlan(
                                PlanExportToTargetRequest.newBuilder()
                                    .setPlan(plan.get())
                                    .setDestination(destination)
                                    .build());

                            return updateDestination(destinationOid, response.getStatus(), marketId);
                        } catch (RuntimeException ex) {
                            // Any error from GRPC call will be a RuntimeException
                            return updateDestination(destinationOid,
                                failExport("Failed calling Topology Processor: " + ex.toString()),
                                marketId);
                        }
                    } else {
                        return updateDestination(destinationOid, rejectExport(errorMessage), marketId);
                    }
                } else {
                    // NOTE: leaves the market ID in the destination as the old one, if any,
                    // not the new bogus one.
                    return updateDestination(destinationOid,
                        rejectExport("Plan market with ID " + request.getMarketId() + " is not found."),
                        null);
                }
            } else {
                // Return the destination with a REJECTED state, but don't update the
                // database since we don't want to overwrite the IN_PROGRESS state.
                // Leave the market as the original (in-progress) one, not the requested new one.

                // Don't send any async notification since it would be confusing to the initiator
                // of the original export, and this caller is getting a synchronous rejection
                // so it doesn't need to get it async too.

                return destinationWithStatusAndMarket(destination,
                    rejectExport("An export to this destination is already in progress."),
                    null);
            }
        } catch (NoSuchObjectException ex) {
            logger.error(DISAPPEARED_DURING_UPDATE, ex);
            return sendDestinationUpdate(destination, failExport(DISAPPEARED_DURING_UPDATE), marketId);
        } catch (IntegrityException ex) {
            logger.error(INTEGRITY_ERROR, ex);
            return sendDestinationUpdate(destination, failExport(INTEGRITY_ERROR), marketId);
        }
    }

    /**
     * Check that a plan export request is valid.
     *
     * @param request the requested export operation
     * @param plan the plan to export
     * @param destination the destination to which to export
     * @return null if the export is allowed, otherwise an error string.
     */
    @Nullable
    public static String validatePlanExport(@Nonnull final PlanExportRequest request,
                                      @Nonnull final PlanInstance plan,
                                      @Nonnull final PlanDestination destination) {
        if (plan.getStatus() != PlanStatus.SUCCEEDED) {
            return "Plan status must be SUCCEEDED to export, but is " + plan.getStatus().toString();
        }

        if (plan.getProjectType() != PlanProjectType.CLOUD_MIGRATION) {
            return "Currently, only cloud migration plans may be exported.";
        }

        if (destination.hasCriteria() && destination.getCriteria().hasAccountId()) {
            long accountId = destination.getCriteria().getAccountId();

            Optional<TopologyMigration> migration = plan.getScenario().getScenarioInfo()
                .getChangesList().stream()
                .filter(ScenarioChange::hasTopologyMigration)
                .map(ScenarioChange::getTopologyMigration)
                .findFirst();

            if (migration.isPresent()) {
                if (migration.get().hasDestinationAccount()) {
                    if (migration.get().getDestinationAccount().getOid() != accountId) {
                        return "Migration is to account ID "
                            + migration.get().getDestinationAccount().getOid()
                            + " but destination requires account with ID " + accountId;
                    }
                } else {
                    return "Migration scenario does not specify an account, while destination "
                        + "requires account with ID " + accountId;
                }
            } else {
                return "Migration plan has no migration scenario change";
            }
        }

        // The request is valid
        return null;
    }

    /**
     * Process a status, by notifying other interested parties and
     * recording the update in the database.
     *
     * @param destinationId the plan destination that has a status update
     * @param status the new status of the destination
     * @param marketId the market which is being exported
     * @return the updated destination
     * @throws NoSuchObjectException when the original plan destination does not exist.
     * @throws IntegrityException when there is an issue with DB.
     */
    public PlanDestination updateDestination(long destinationId,
                                             @Nonnull PlanExportStatus status,
                                             @Nullable Long marketId)
        throws NoSuchObjectException, IntegrityException {

        updateLock.lock();
        try {
            PlanDestination updatedDestination =
                planDestinationDao.updatePlanDestination(destinationId, status, marketId);

            try {
                if (status.getState() == PlanExportState.IN_PROGRESS) {
                    planExportNotificationSender.onPlanDestinationProgress(updatedDestination);
                } else {
                    planExportNotificationSender.onPlanDestinationStateChanged(updatedDestination);
                }
            } catch (PlanExportListenerException ex) {
                logger.error("Unexpected error while trying to send update {} for export to "
                    + " destination {}:", status.toString(), destinationId, ex);
            }

            return updatedDestination;
        } finally {
            updateLock.unlock();
        }
    }

    /**
     * Send a status update, without updating the database, for
     * use in cases where we can't (because we're trying to report a DB update failure)
     * or shouldn't (there is an export in progress) update the DB.
     *
     * @param destination the destination about which to send an update
     * @param status the new status of the destination
     * @param marketId the market associated with the destination, or null if no change.
     * @return the updated destination refecting what was notified
     */
    private PlanDestination sendDestinationUpdate(@Nonnull PlanDestination destination,
                                                  @Nonnull PlanExportStatus status,
                                                  @Nullable Long marketId) {
        PlanDestination updatedDestination =
            destinationWithStatusAndMarket(destination, status, marketId);

        try {
            planExportNotificationSender.onPlanDestinationStateChanged(updatedDestination);
        } catch (PlanExportListenerException ex) {
            logger.error("Unexpected error while trying to send status update {} for export to "
                + " destination {}:", status.toString(), destination.getOid(), ex);
        }

        return updatedDestination;
    }

    /**
     * Returns a destination with the status, and optionally market ID, updated.
     *
     * @param destination the current destination value
     * @param status the new status to apply
     * @param marketId update the market ID to this value if non-null, else do not update.
     *
     * @return a new destination with the new value(s)
     */
    private PlanDestination destinationWithStatusAndMarket(
        @Nonnull final PlanDestination destination,
        @Nonnull final PlanExportStatus status,
        @Nonnull final Long marketId) {
        Builder builder = PlanDestination.newBuilder(destination)
            .setStatus(status);

        if (marketId != null) {
            builder.setMarketId(marketId);
        }

        return builder.build();
    }

    @Override
    public void onPlanExportProgress(final long planDestinationOid, final int progress, final String message) {
        PlanExportStatus status = PlanExportStatus.newBuilder()
            .setState(PlanExportState.IN_PROGRESS)
            .setProgress(progress)
            .setDescription(message)
            .build();

        try {
            updateDestination(planDestinationOid, status, null);
        } catch (NoSuchObjectException ex) {
            logger.error(DISAPPEARED_DURING_UPDATE, ex);
        } catch (IntegrityException ex) {
            logger.error(INTEGRITY_ERROR, ex);
        }
    }

    @Override
    public void onPlanExportStateChanged(final long planDestinationOid, @Nonnull final PlanExportStatus status) {
        try {
            updateDestination(planDestinationOid, status, null);
        } catch (NoSuchObjectException ex) {
            logger.error(DISAPPEARED_DURING_UPDATE, ex);
        } catch (IntegrityException ex) {
            logger.error(INTEGRITY_ERROR, ex);
        }
    }

    private PlanExportStatus rejectExport(@Nonnull String message) {
        return PlanExportStatus.newBuilder()
            .setState(PlanExportState.REJECTED)
            .setDescription(message)
            .setProgress(0)
            .build();
    }

    private PlanExportStatus failExport(@Nonnull String message) {
        return PlanExportStatus.newBuilder()
            .setState(PlanExportState.FAILED)
            .setDescription(message)
            .setProgress(0)
            .build();
    }
}