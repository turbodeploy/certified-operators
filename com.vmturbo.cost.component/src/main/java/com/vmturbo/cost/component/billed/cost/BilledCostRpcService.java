package com.vmturbo.cost.component.billed.cost;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostBucket;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostData;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostItem;
import com.vmturbo.common.protobuf.cost.BilledCostServiceGrpc.BilledCostServiceImplBase;
import com.vmturbo.common.protobuf.cost.BilledCostServices.GetBilledCostStatsRequest;
import com.vmturbo.common.protobuf.cost.BilledCostServices.GetBilledCostStatsResponse;
import com.vmturbo.common.protobuf.cost.BilledCostServices.UploadBilledCloudCostRequest;
import com.vmturbo.common.protobuf.cost.BilledCostServices.UploadBilledCloudCostRequest.BilledCostSegment;
import com.vmturbo.common.protobuf.cost.BilledCostServices.UploadBilledCloudCostRequest.SegmentCase;
import com.vmturbo.common.protobuf.cost.BilledCostServices.UploadBilledCloudCostResponse;
import com.vmturbo.cost.component.billed.cost.CloudCostStore.BilledCostPersistenceSession;

/**
 * Billed cost RPC service for querying billed cost data.
 */
public class BilledCostRpcService extends BilledCostServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final CloudCostStore cloudCostStore;

    /**
     * Contructs a new {@link BilledCostRpcService} instance.
     * @param cloudCostStore The cloud cost (billed cost) store.
     */
    public BilledCostRpcService(@Nonnull CloudCostStore cloudCostStore) {
        this.cloudCostStore = Objects.requireNonNull(cloudCostStore);
    }

    @Override
    public void getBilledCostStats(GetBilledCostStatsRequest request,
                                   StreamObserver<GetBilledCostStatsResponse> responseObserver) {
        try {

            Preconditions.checkArgument(request.hasQuery(), "Billed cost request must contain a query");

            final Stopwatch stopwatch = Stopwatch.createStarted();
            responseObserver.onNext(GetBilledCostStatsResponse.newBuilder()
                    .addAllCostStats(cloudCostStore.getCostStats(request.getQuery()))
                    .build());

            logger.info("Responding to the following query in {}:\n{}", stopwatch, request);

            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asException());
        }
    }

    @Override
    public StreamObserver<UploadBilledCloudCostRequest> uploadBilledCloudCost(
            final StreamObserver<UploadBilledCloudCostResponse> responseObserver) {

        return new UploadBilledCostRequestHandler(cloudCostStore, responseObserver);
    }

    /**
     * Handler for streamed requests to upload billed cost.
     */
    private class UploadBilledCostRequestHandler
            implements StreamObserver<UploadBilledCloudCostRequest> {

        private final BilledCostData.Builder billedCostData;

        private final StreamObserver<UploadBilledCloudCostResponse> responseObserver;

        private final BilledCostPersistenceSession persistenceSession;

        private final Set<SegmentCase> segmentsReceived;

        /**
         * Constructor for {@link UploadBilledCostRequestHandler}.
         *
         * @param cloudCostStore store in which to persist billed cost as it is received
         * @param responseObserver stream to emit responses to
         */
        UploadBilledCostRequestHandler(final CloudCostStore cloudCostStore,
                final StreamObserver<UploadBilledCloudCostResponse> responseObserver) {
            this.responseObserver = responseObserver;
            billedCostData = BilledCostData.newBuilder();
            persistenceSession = cloudCostStore.createPersistenceSession();
            segmentsReceived = new HashSet<>();
        }

        @Override
        public void onNext(final UploadBilledCloudCostRequest request) {

            segmentsReceived.add(request.getSegmentCase());

            switch (request.getSegmentCase()) {
                case BILLED_COST_METADATA:
                    handleBilledCostMetadataRequest(request);
                    break;
                case COST_TAGS:
                    handleCostTagsRequest(request);
                    break;
                case BILLED_COST:
                    handleBilledCostRequest(request);
                    break;
                default:
                    break;
            }
        }

        private void handleBilledCostMetadataRequest(final UploadBilledCloudCostRequest request) {
            billedCostData.setBillingFamilyId(request.getBilledCostMetadata().getBillingFamilyId());
            billedCostData.setServiceProviderId(
                    request.getBilledCostMetadata().getServiceProviderId());
            billedCostData.setGranularity(request.getBilledCostMetadata().getGranularity());
        }

        private void handleCostTagsRequest(final UploadBilledCloudCostRequest request) {
            billedCostData.putAllCostTagGroup(request.getCostTags().getCostTagGroupMap());
        }

        private void handleBilledCostRequest(final UploadBilledCloudCostRequest request) {
            if (!segmentsReceived.contains(SegmentCase.COST_TAGS) || !segmentsReceived.contains(
                    SegmentCase.BILLED_COST_METADATA)) {
                responseObserver.onError(Status.INTERNAL.withDescription(
                                "Billed cost buckets received before metadata or cost tag groups")
                        .asException());
                return;
            } else if (!allReferencedTagGroupsExist(request.getBilledCost())) {
                responseObserver.onError(Status.INTERNAL.withDescription(
                                "Billed cost buckets refer to tag groups which have not been received")
                        .asException());
                return;
            }

            // Persist cost buckets as they are received
            // Note: yes, including tag groups and metadata every time is redundant,
            // but there is a tags cache
            try {
                persistenceSession.storeCostDataAsync(billedCostData.clearCostBuckets()
                        .addAllCostBuckets(request.getBilledCost().getCostBucketsList())
                        .build());
            } catch (final Exception exception) {
                responseObserver.onError(Status.INTERNAL.withCause(exception).asException());
            }
        }

        private boolean allReferencedTagGroupsExist(final BilledCostSegment billedCostSegment) {
            return billedCostSegment.getCostBucketsList()
                    .stream()
                    .map(BilledCostBucket::getCostItemsList)
                    .flatMap(Collection::stream)
                    .filter(BilledCostItem::hasCostTagGroupId)
                    .map(BilledCostItem::getCostTagGroupId)
                    .allMatch(billedCostData::containsCostTagGroup);
        }

        @Override
        public void onError(final Throwable error) {
            logger.error("Error while receiving billed cost upload: ", error);
        }

        @Override
        public void onCompleted() {
            try {
                // Wait on any pending persistence operations to wrap up
                persistenceSession.commitSession();

                // Complete the request
                responseObserver.onNext(UploadBilledCloudCostResponse.getDefaultInstance());
                responseObserver.onCompleted();
            } catch (final Exception exception) {
                logger.error("Error persisting uploaded billed cost data: ", exception);
                responseObserver.onError(Status.INTERNAL.withCause(exception).asException());
            }
        }
    }
}
