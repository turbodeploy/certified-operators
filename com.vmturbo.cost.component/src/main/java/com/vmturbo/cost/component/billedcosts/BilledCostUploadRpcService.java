package com.vmturbo.cost.component.billedcosts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.BilledCostUploadServiceGrpc;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.platform.sdk.common.CostBilling;
import com.vmturbo.sql.utils.DbException;

/**
 * Implementation of the BilledCostUploadService. Ingests Billing data points into the Billed Cost Hourly, Daily and
 * Monthly tables. Ensures referential integrity between Tag, Tag group and Billed Cost tables by resolving tag ids and
 * tag group ids in a consistent manner.
 */
public class BilledCostUploadRpcService extends BilledCostUploadServiceGrpc.BilledCostUploadServiceImplBase {

    private static final Logger logger = LogManager.getLogger();
    private final TagGroupIdentityService tagGroupIdentityService;
    private final BilledCostStore billedCostStore;

    /**
     * Creates an instance of BilledCostUploadRpcService.
     *
     * @param tagGroupIdentityService used to resolve tag group ids by creating new ones or retrieving existing ones.
     * @param billedCostStore to insert into billed cost tables.
     *
     */
    public BilledCostUploadRpcService(@Nonnull final TagGroupIdentityService tagGroupIdentityService,
                                      @Nonnull final BilledCostStore billedCostStore) {
        this.tagGroupIdentityService = Objects.requireNonNull(tagGroupIdentityService);
        this.billedCostStore = Objects.requireNonNull(billedCostStore);
    }

    @Override
    public StreamObserver<Cost.UploadBilledCostRequest> uploadBilledCost(
        @Nonnull final StreamObserver<Cost.UploadBilledCostResponse> responseObserver) {
        return new BilledCostRequestStreamObserver(responseObserver, tagGroupIdentityService, billedCostStore);
    }

    /**
     * Stream observer implementation for processing UploadBilledCostRequests.
     */
    private static class BilledCostRequestStreamObserver implements StreamObserver<Cost.UploadBilledCostRequest> {

        private final StreamObserver<Cost.UploadBilledCostResponse> responseObserver;
        private final TagGroupIdentityService tagGroupIdentityService;
        private final BilledCostStore billedCostStore;

        private final Map<String, List<Cost.UploadBilledCostRequest.BillingDataPoint>>
            deferredBillingDataPointsByBillingId = new HashMap<>();
        private final Map<String, Map<Long, CostBilling.CostTagGroup>> deferredTagGroupsByBillingId = new HashMap<>();
        private final Map<String, CostBilling.CloudBillingData.CloudBillingBucket.Granularity>
            granularityByBillingId = new HashMap<>();
        private final List<Future<Integer>> submittedBatches = new ArrayList<>();
        private State state = State.INITIALIZED;

        /**
         * States of the Stream observer.
         */
        private enum State {
            /**
             * Initial state of the Stream observer.
             */
            INITIALIZED,
            /**
             * Final state of stream observer, reached when onCompleted or onError message is processed OR whenever an
             * onError is sent to the client when an exception is encountered.
             */
            COMPLETE,
            /**
             * This state is reached from INITIALIZED when onNext message is received.
             */
            IN_PROGRESS
        }

        BilledCostRequestStreamObserver(@Nonnull StreamObserver<Cost.UploadBilledCostResponse> responseObserver,
                                        @Nonnull TagGroupIdentityService tagGroupIdentityService,
                                        @Nonnull BilledCostStore billedCostStore) {
            this.responseObserver = Objects.requireNonNull(responseObserver);
            this.tagGroupIdentityService = Objects.requireNonNull(tagGroupIdentityService);
            this.billedCostStore = Objects.requireNonNull(billedCostStore);
        }

        @Override
        public void onNext(Cost.UploadBilledCostRequest uploadBilledCostRequest) {
            if (state == State.COMPLETE) {
                return;
            }
            if (state == State.INITIALIZED) {
                logger.info("Start processing BilledCostUpload requests.");
                state = State.IN_PROGRESS;
            }
            if (uploadBilledCostRequest.hasBillingIdentifier() && uploadBilledCostRequest.hasGranularity()) {
                final String billingId = uploadBilledCostRequest.getBillingIdentifier();
                final List<Cost.UploadBilledCostRequest.BillingDataPoint> points = uploadBilledCostRequest
                    .getSamplesList();
                final Map<Long, CostBilling.CostTagGroup> tagGroupByGroupId = uploadBilledCostRequest
                    .getCostTagGroupMapList().stream()
                    .collect(Collectors.toMap(
                        Cost.UploadBilledCostRequest.CostTagGroupMap::getGroupId,
                        Cost.UploadBilledCostRequest.CostTagGroupMap::getTags));
                final CostBilling.CloudBillingData.CloudBillingBucket.Granularity granularity =
                    uploadBilledCostRequest.getGranularity();
                if (uploadBilledCostRequest.getAtomicRequest()) {
                    try {
                        final Map<Long, Long> discoveredTagGroupIdToOid = tagGroupIdentityService
                            .resolveIdForDiscoveredTagGroups(tagGroupByGroupId);
                        submittedBatches.addAll(billedCostStore.insertBillingDataPoints(points,
                            discoveredTagGroupIdToOid, granularity));
                    } catch (DbException exception) {
                        logger.error("Exception encountered processing Billed Cost Upload request.", exception);
                        state = State.COMPLETE;
                        responseObserver.onError(exception);
                    }
                } else {
                    // request is not atomic, defer processing to onComplete call
                    deferredBillingDataPointsByBillingId.computeIfAbsent(billingId, (k) -> new ArrayList<>())
                        .addAll(points);
                    deferredTagGroupsByBillingId.computeIfAbsent(billingId, (k) -> new HashMap<>())
                        .putAll(tagGroupByGroupId);
                    granularityByBillingId.putIfAbsent(billingId, granularity);
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {
            state = State.COMPLETE;
            logger.error("BilledCostUploadRpcService received onError message: ", throwable);
        }

        @Override
        public void onCompleted() {
            if (state == State.COMPLETE) {
                return;
            }
            try {
                for (Map.Entry<String, List<Cost.UploadBilledCostRequest.BillingDataPoint>> entry
                    : deferredBillingDataPointsByBillingId.entrySet()) {
                    final String billingId = entry.getKey();
                    List<Cost.UploadBilledCostRequest.BillingDataPoint> points = entry.getValue();
                    final Map<Long, CostBilling.CostTagGroup> tagGroupsByTagGroupId = deferredTagGroupsByBillingId
                        .getOrDefault(billingId, new HashMap<>());
                    final Map<Long, Long> discoveredTagGroupIdToOid = tagGroupIdentityService
                        .resolveIdForDiscoveredTagGroups(tagGroupsByTagGroupId);
                    final CostBilling.CloudBillingData.CloudBillingBucket.Granularity granularity =
                        granularityByBillingId.get(billingId);
                    submittedBatches.addAll(billedCostStore.insertBillingDataPoints(points, discoveredTagGroupIdToOid,
                        granularity));
                }
                final Cost.UploadBilledCostResponse.Builder response = Cost.UploadBilledCostResponse.newBuilder();
                int totalItemsProcessed = 0;
                for (final Future<Integer> batch : submittedBatches) {
                    totalItemsProcessed += batch.get();
                }
                logger.info("Completed persisting {} billing data items.", totalItemsProcessed);
                responseObserver.onNext(response.build());
                responseObserver.onCompleted();
            } catch (final DbException | ExecutionException | InterruptedException  exception) {
                logger.error("Exception encountered processing Billed Cost Upload request.", exception);
                responseObserver.onError(exception);
                if (exception instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            } finally {
                state = State.COMPLETE;
            }
        }
    }
}